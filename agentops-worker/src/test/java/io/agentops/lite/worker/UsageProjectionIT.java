package io.agentops.lite.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.UsageLedgerEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Verifies Kafka at-least-once delivery cannot double-apply an immutable ledger event. */
@Testcontainers
@SpringBootTest(classes = AgentOpsWorkerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "agentops.worker.relay-delay-ms=3600000", "agentops.worker.recovery-delay-ms=3600000"
})
class UsageProjectionIT {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("agentops").withUsername("agentops").withPassword("agentops");

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379);

    @Container
    // Match docker-compose so local verification reuses the same cached broker image.
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    @DynamicPropertySource
    static void registerInfrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.flyway.enabled", () -> true);
        // Worker production runtime does not own migrations; tests reuse the Server-owned canonical schema.
        registry.add("spring.flyway.locations", () -> "filesystem:"
                + Path.of("..", "agentops-server", "src", "main", "resources", "db", "migration").toAbsolutePath());
    }

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private ObjectMapper mapper;

    /** Clears projection facts so every test starts from the same migrated schema. */
    @BeforeEach
    void resetProjection() {
        jdbc.update("delete from usage_projection_applied");
        jdbc.update("delete from usage_projection");
    }

    /** Publishes the same event twice and expects a single projection application. */
    @Test
    void appliesRedeliveredLedgerEventExactlyOnce() throws Exception {
        String ledgerId = UUID.randomUUID().toString();
        UsageLedgerEvent event = new UsageLedgerEvent(ledgerId, "project-fund-agent", UUID.randomUUID().toString(),
                "USAGE_ACTUAL", 77, BigDecimal.ZERO, "fund-agent-stable-v1", Instant.now());
        String payload = mapper.writeValueAsString(event);

        kafka.send("agentops.usage.ledger.v1", ledgerId, payload).get();
        kafka.send("agentops.usage.ledger.v1", ledgerId, payload).get();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(jdbc.queryForObject("select count(*) from usage_projection_applied where ledger_id=?", Integer.class, ledgerId)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select total_tokens from usage_projection where project_id='project-fund-agent'", Long.class)).isEqualTo(77);
        });
    }
}
