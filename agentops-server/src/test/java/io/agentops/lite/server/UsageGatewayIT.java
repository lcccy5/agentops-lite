package io.agentops.lite.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Verifies the online gateway against real MySQL and Redis boundaries.
 * A tiny JDK HTTP server controls only the external model response so persistence, Lua and cancellation remain real.
 */
@Testcontainers
@SpringBootTest(classes = AgentOpsServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class UsageGatewayIT {
    private static final String PROJECT_ID = "project-fund-agent";
    private static final String ADMIN_TOKEN = "local-admin-token";
    private static final String API_KEY = "agentops-dev-key";

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("agentops").withUsername("agentops").withPassword("agentops");

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379);

    private static final ExecutorService PROVIDER_EXECUTOR = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "usage-gateway-it-provider");
        thread.setDaemon(true);
        return thread;
    });
    private static final HttpServer PROVIDER = startProvider();
    private static volatile ProviderMode providerMode = ProviderMode.ORDINARY;
    private static volatile String providerResponseBody = "{}";

    private enum ProviderMode { ORDINARY, SLOW_STREAM }

    @DynamicPropertySource
    static void registerInfrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("agentops.provider-base-url", UsageGatewayIT::providerBaseUrl);
        registry.add("agentops.reservation-timeout", () -> "5s");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private WebTestClient client;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private ObjectMapper mapper;

    /** Resets mutable facts while preserving Flyway's local project and API-key seed. */
    @BeforeEach
    void resetMutableState() {
        providerMode = ProviderMode.ORDINARY;
        providerResponseBody = "{}";
        jdbc.update("delete from usage_reconciliation");
        jdbc.update("delete from usage_projection_applied");
        jdbc.update("delete from usage_projection");
        jdbc.update("delete from usage_outbox");
        jdbc.update("delete from usage_ledger");
        jdbc.update("delete from usage_reservation");
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
    }

    /** Stops the in-process provider after all Spring contexts release their clients. */
    @AfterAll
    static void stopProvider() {
        PROVIDER.stop(0);
        PROVIDER_EXECUTOR.shutdownNow();
    }

    /** Proves provider usage becomes one immutable ledger entry and releases the Redis permit. */
    @Test
    void settlesProviderUsageWithoutLeakingQuota() {
        stubOrdinaryUsage(28, 12);
        String requestId = UUID.randomUUID().toString();

        sendOrdinary(requestId).expectStatus().isOk();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Map<String, Object> reservation = queryReservation(requestId);
            assertThat(reservation.get("status")).isEqualTo("SETTLED");
            assertThat(((Number) reservation.get("actual_tokens")).longValue()).isEqualTo(40);
            assertThat(jdbc.queryForObject("select count(*) from usage_ledger where reservation_id=?", Integer.class,
                    reservation.get("reservation_id"))).isEqualTo(1);
            assertThat(redisCounter("active")).isZero();
            assertThat(redisCounter("reserved")).isZero();
            assertThat(redisCounter("consumed")).isEqualTo(40);
        });
    }

    /** Proves a repeated business key cannot create a second reservation, ledger or charge. */
    @Test
    void rejectsFinalizedIdempotencyKeyWithoutDoubleCharging() {
        stubOrdinaryUsage(20, 10);
        String requestId = UUID.randomUUID().toString();
        sendOrdinary(requestId).expectStatus().isOk();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(queryReservation(requestId).get("status")).isEqualTo("SETTLED"));

        sendOrdinary(requestId).expectStatus().isEqualTo(409);

        assertThat(jdbc.queryForObject("select count(*) from usage_reservation where idempotency_key=?", Integer.class, requestId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from usage_ledger", Integer.class)).isEqualTo(1);
        assertThat(redisCounter("consumed")).isEqualTo(30);
    }

    /** Proves cancelling a partial SSE response still closes quota and persists an auditable estimate. */
    @Test
    void cancelsPartialStreamWithoutLeakingQuota() {
        providerMode = ProviderMode.SLOW_STREAM;
        String requestId = UUID.randomUUID().toString();
        JsonNode request = mapper.valueToTree(Map.of("model", "deterministic-fund-model", "stream", true,
                "max_tokens", 128, "messages", new Object[]{Map.of("role", "user", "content", "分析基金风险")}));

        WebClient.create("http://localhost:" + port).post().uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + API_KEY)
                .header("Idempotency-Key", requestId).header("X-AgentOps-Request-Id", requestId)
                .contentType(MediaType.APPLICATION_JSON).bodyValue(request).retrieve()
                .bodyToFlux(String.class).take(1).blockLast(Duration.ofSeconds(10));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Map<String, Object> reservation = queryReservation(requestId);
            assertThat(reservation.get("status")).isIn("CANCELLED", "RECONCILIATION_PENDING");
            assertThat(redisCounter("active")).isZero();
            assertThat(redisCounter("reserved")).isZero();
            assertThat(jdbc.queryForObject("select count(*) from usage_ledger", Integer.class)).isEqualTo(1);
        });
    }

    /** Configures one deterministic non-streaming provider response with authoritative usage. */
    private void stubOrdinaryUsage(long inputTokens, long outputTokens) {
        providerMode = ProviderMode.ORDINARY;
        // A literal response keeps the provider contract visible in the failure report.
        providerResponseBody = "{\"id\":\"chatcmpl-it\",\"choices\":[{\"message\":{\"content\":\"ok\"}}],"
                + "\"usage\":{\"prompt_tokens\":" + inputTokens
                + ",\"completion_tokens\":" + outputTokens + "}}";
    }

    /** Sends a deterministic request whose request ID also serves as the idempotency key. */
    private WebTestClient.ResponseSpec sendOrdinary(String requestId) {
        return client.post().uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + API_KEY)
                .header("Idempotency-Key", requestId).header("X-AgentOps-Request-Id", requestId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("model", "deterministic-fund-model", "stream", false, "max_tokens", 128,
                        "messages", new Object[]{Map.of("role", "user", "content", "分析基金风险")}))
                .exchange();
    }

    /** Returns the persisted reservation using the public diagnostic contract. */
    private Map<String, Object> queryReservation(String requestId) {
        return client.get().uri("/internal/v1/usage/queryRequest/{requestId}", requestId)
                .header("X-AgentOps-Admin-Token", ADMIN_TOKEN).exchange().expectStatus().isOk()
                .expectBody(new org.springframework.core.ParameterizedTypeReference<Map<String, Object>>() { }).returnResult().getResponseBody();
    }

    /** Reads one Redis quota counter and treats an absent counter as zero. */
    private long redisCounter(String field) {
        Object value = redis.opsForHash().get("agentops:quota:" + PROJECT_ID, field);
        return value == null ? 0 : Long.parseLong(value.toString());
    }

    /** Starts a dependency-free provider double before Spring resolves dynamic properties. */
    private static HttpServer startProvider() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/v1/chat/completions", UsageGatewayIT::handleProviderRequest);
            server.setExecutor(PROVIDER_EXECUTOR);
            server.start();
            return server;
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot start the integration-test provider", exception);
        }
    }

    /** Returns the OpenAI-compatible base URL exposed by the in-process provider double. */
    private static String providerBaseUrl() {
        return "http://localhost:" + PROVIDER.getAddress().getPort();
    }

    /** Routes each model request to the ordinary or deliberately slow streaming fixture. */
    private static void handleProviderRequest(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getRequestBody().readAllBytes();
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            if (providerMode == ProviderMode.SLOW_STREAM) {
                writeSlowStream(exchange);
                return;
            }
            byte[] body = providerResponseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
        }
    }

    /** Flushes one SSE event before delaying the remainder so the client can cancel mid-stream. */
    private static void writeSlowStream(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE);
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"
                    .getBytes(StandardCharsets.UTF_8));
            output.flush();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
            output.write(("data: {\"choices\":[{\"delta\":{\"content\":\"late\"}}]}\n\n"
                    + "data: [DONE]\n\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException ignored) {
            // A closed socket is expected after the gateway cancels the upstream response.
        }
    }
}
