package io.agentops.lite.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.UsageLedgerEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.transaction.support.TransactionTemplate;
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
        "agentops.worker.relay-delay-ms=3600000", "agentops.worker.recovery-delay-ms=3600000",
        "agentops.worker.kafka-retry-max-attempts=3", "agentops.worker.kafka-retry-backoff=10ms"
})
class UsageProjectionIT {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("agentops").withUsername("agentops").withPassword("agentops")
            .withCommand("--log-bin-trust-function-creators=1");

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

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private UsageWorker usageWorker;

    @Autowired
    private TransactionTemplate transactions;

    @Autowired
    private WorkerProperties workerProperties;

    @Autowired
    private UsageQuotaWorker quotaWorker;

    /** Clears projection facts so every test starts from the same migrated schema. */
    @BeforeEach
    void resetProjection() {
        jdbc.update("delete from usage_lookup_task");
        jdbc.update("delete from usage_quota_task");
        jdbc.update("delete from usage_provider_attempt");
        jdbc.update("delete from usage_outbox");
        jdbc.update("delete from usage_ledger");
        jdbc.update("delete from usage_reservation");
        jdbc.update("delete from usage_reconciliation");
        jdbc.update("delete from usage_projection_applied");
        jdbc.update("delete from usage_projection");
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
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
            System.out.printf("Kafka duplicate-message result: ledgerId=%s, sent=2, applied=1, projectedTokens=77%n", ledgerId);
        });
    }

    /** Makes Kafka send fail first, then proves all pending Outbox facts are retried and projected. */
    @Test
    void retriesOutboxAfterKafkaSendFailure() throws Exception {
        final int eventCount = Integer.getInteger("billing.sample.count", 10);
        List<String> ledgerIds = new ArrayList<>();
        for (int index = 0; index < eventCount; index++) ledgerIds.add(insertPendingOutboxEvent(40));

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> unavailableKafka = mock(KafkaTemplate.class);
        when(unavailableKafka.send(anyString(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("injected kafka unavailable"));
        new UsageWorker(jdbc, transactions, unavailableKafka, redis, mapper, workerProperties).relayUsageOutbox();

        assertThat(jdbc.queryForObject("select count(*) from usage_outbox where status='PENDING' and attempts=1", Integer.class))
                .isEqualTo(eventCount);
        jdbc.update("update usage_outbox set next_attempt_at=? where status='PENDING'", Instant.now().minusSeconds(1));
        usageWorker.relayUsageOutbox();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(jdbc.queryForObject("select count(*) from usage_outbox where status='PUBLISHED'", Integer.class))
                    .isEqualTo(eventCount);
            assertThat(jdbc.queryForObject("select count(*) from usage_projection_applied", Integer.class))
                    .isEqualTo(eventCount);
            assertThat(jdbc.queryForObject("select total_tokens from usage_projection where project_id='project-fund-agent'", Long.class))
                    .isEqualTo(eventCount * 40L);
        });
        System.out.printf("Outbox retry result: kafkaSendFailures=%d, retried=%d, published=%d, projectedTokens=%d%n",
                eventCount, eventCount, eventCount, eventCount * 40L);
    }

    /** Fails the first MySQL projection write, then verifies retry and duplicate delivery still apply each ledger once. */
    @Test
    void retriesKafkaConsumptionAfterMySqlProjectionFailure() throws Exception {
        final int eventCount = Integer.getInteger("billing.sample.count", 10);
        List<String> payloads = new ArrayList<>();
        for (int index = 0; index < eventCount; index++) {
            UsageLedgerEvent event = new UsageLedgerEvent(UUID.randomUUID().toString(), "project-fund-agent",
                    UUID.randomUUID().toString(), "USAGE_ACTUAL", 40, BigDecimal.ZERO, "projection-test", Instant.now());
            payloads.add(mapper.writeValueAsString(event));
        }

        jdbc.execute("drop trigger if exists usage_projection_insert_fail");
        jdbc.execute("create trigger usage_projection_insert_fail before insert on usage_projection for each row "
                + "signal sqlstate '45000' set message_text = 'injected projection failure'");
        try {
            assertThatThrownBy(() -> usageWorker.applyUsageProjection(payloads.getFirst())).isNotNull();
            assertThat(jdbc.queryForObject("select count(*) from usage_projection_applied", Integer.class)).isZero();
            assertThat(jdbc.queryForObject("select count(*) from usage_projection", Integer.class)).isZero();
        } finally {
            jdbc.execute("drop trigger if exists usage_projection_insert_fail");
        }

        for (String payload : payloads) usageWorker.applyUsageProjection(payload);
        for (String payload : payloads) usageWorker.applyUsageProjection(payload);
        assertThat(jdbc.queryForObject("select count(*) from usage_projection_applied", Integer.class)).isEqualTo(eventCount);
        assertThat(jdbc.queryForObject("select total_tokens from usage_projection where project_id='project-fund-agent'", Long.class))
                .isEqualTo(eventCount * 40L);
        System.out.printf("Kafka consumer recovery result: failedBeforeCommit=1, recoveredEvents=%d, duplicateReplays=%d, projectedTokens=%d%n",
                eventCount, eventCount, eventCount * 40L);
    }

    /** Routes a permanently invalid projection to the DLT instead of blocking its source partition forever. */
    @Test
    void routesPoisonUsageEventToDeadLetterTopicAfterFiniteRetries() throws Exception {
        String ledgerId = UUID.randomUUID().toString();
        String invalidProjectId = "x".repeat(65);
        UsageLedgerEvent event = new UsageLedgerEvent(ledgerId, invalidProjectId, UUID.randomUUID().toString(),
                "USAGE_ACTUAL", 40, BigDecimal.ZERO, "poison-test", Instant.now());
        String payload = mapper.writeValueAsString(event);

        kafka.send("agentops.usage.ledger.v1", ledgerId, payload).get();

        Map<String, Object> consumerProperties = new HashMap<>();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "usage-dlt-verifier-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ConsumerRecord<String, String> deadLetter = null;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(List.of("agentops.usage.ledger.v1.DLT"));
            Instant deadline = Instant.now().plusSeconds(20);
            while (deadLetter == null && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (ledgerId.equals(record.key())) {
                        deadLetter = record;
                        break;
                    }
                }
            }
        }

        assertThat(deadLetter).isNotNull();
        assertThat(deadLetter.value()).isEqualTo(payload);
        assertThat(deadLetter.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
        assertThat(deadLetter.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull();
        assertThat(jdbc.queryForObject("select count(*) from usage_projection_applied where ledger_id=?", Integer.class, ledgerId))
                .isZero();
        assertThat(jdbc.queryForObject("select count(*) from usage_projection where project_id=?", Integer.class, invalidProjectId))
                .isZero();
    }

    /** Creates one immutable ledger fact and its matching PENDING Outbox row for retry testing. */
    private String insertPendingOutboxEvent(long tokens) throws Exception {
        String ledgerId = UUID.randomUUID().toString();
        String reservationId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        UsageLedgerEvent event = new UsageLedgerEvent(ledgerId, "project-fund-agent", reservationId,
                "USAGE_ACTUAL", tokens, BigDecimal.ZERO, "outbox-test", now);
        jdbc.update("insert into usage_ledger(ledger_id,reservation_id,project_id,ledger_type,token_delta,cost_delta,prompt_version,occurred_at) values(?,?,?,?,?,0,?,?)",
                ledgerId, reservationId, "project-fund-agent", "USAGE_ACTUAL", tokens, "outbox-test", now);
        jdbc.update("insert into usage_outbox(event_id,ledger_id,event_key,payload_json,status,next_attempt_at,created_at) values(?,?,?,?, 'PENDING',?,?)",
                UUID.randomUUID().toString(), ledgerId, ledgerId, mapper.writeValueAsString(event), now.minusSeconds(1), now);
        return ledgerId;
    }
    /** Expires an unstarted reservation and proves the durable compensation task releases its Redis quota. */
    @Test
    void compensatesExpiredReservationWithoutLeakingQuota() {
        String reservationId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        long reservedTokens = 200;
        jdbc.update("""
                insert into usage_reservation(reservation_id,request_id,correlation_id,project_id,idempotency_key,
                    reserved_tokens,status,expires_at,created_at,updated_at)
                values(?,?,?,?,?,?,'RESERVED',?,?,?)
                """, reservationId, UUID.randomUUID().toString(), reservationId, "project-fund-agent",
                "timeout-test-" + reservationId, reservedTokens, now.minusSeconds(1), now.minusSeconds(2), now.minusSeconds(2));
        String quotaKey = "agentops:quota:project-fund-agent";
        String reservationKey = "agentops:reservation:" + reservationId;
        redis.opsForHash().put(quotaKey, "reserved", Long.toString(reservedTokens));
        redis.opsForHash().put(quotaKey, "active", "1");
        redis.opsForHash().put(quotaKey, "consumed", "0");
        redis.opsForHash().put(reservationKey, "state", "RESERVED");
        redis.opsForHash().put(reservationKey, "tokens", Long.toString(reservedTokens));

        usageWorker.reconcileUsage();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            quotaWorker.applyPendingQuotaTasks();
            assertThat(jdbc.queryForObject("select status from usage_reservation where reservation_id=?", String.class, reservationId))
                    .isEqualTo("CANCELLED");
            assertThat(jdbc.queryForObject("select settlement_status from usage_reservation where reservation_id=?", String.class, reservationId))
                    .isEqualTo("FINAL");
            assertThat(jdbc.queryForObject("select status from usage_quota_task where reservation_id=?", String.class, reservationId))
                    .isEqualTo("APPLIED");
            assertThat(redisCounter(quotaKey, "reserved")).isZero();
            assertThat(redisCounter(quotaKey, "active")).isZero();
            assertThat(redisCounter(quotaKey, "consumed")).isZero();
        });
        System.out.printf("Expired reservation result: reservedTokens=%d, reservation=CANCELLED, redisReserved=0, redisActive=0%n", reservedTokens);
    }

    /** Runs two recovery scans together and proves they create one compensation task and one Redis refund. */
    @Test
    void doesNotDoubleCompensateWhenRecoveryScansRace() throws Exception {
        String reservationId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        long reservedTokens = 200;
        jdbc.update("""
                insert into usage_reservation(reservation_id,request_id,correlation_id,project_id,idempotency_key,
                    reserved_tokens,status,expires_at,created_at,updated_at)
                values(?,?,?,?,?,?,'RESERVED',?,?,?)
                """, reservationId, UUID.randomUUID().toString(), reservationId, "project-fund-agent",
                "racing-recovery-" + reservationId, reservedTokens, now.minusSeconds(1), now.minusSeconds(2), now.minusSeconds(2));
        String quotaKey = "agentops:quota:project-fund-agent";
        String reservationKey = "agentops:reservation:" + reservationId;
        redis.opsForHash().put(quotaKey, "reserved", Long.toString(reservedTokens));
        redis.opsForHash().put(quotaKey, "active", "1");
        redis.opsForHash().put(quotaKey, "consumed", "0");
        redis.opsForHash().put(reservationKey, "state", "RESERVED");
        redis.opsForHash().put(reservationKey, "tokens", Long.toString(reservedTokens));

        CountDownLatch clientsReady = new CountDownLatch(2);
        CountDownLatch startTogether = new CountDownLatch(1);
        CompletableFuture<Void> first = CompletableFuture.runAsync(() -> runRecoveryScan(clientsReady, startTogether));
        CompletableFuture<Void> second = CompletableFuture.runAsync(() -> runRecoveryScan(clientsReady, startTogether));
        assertThat(clientsReady.await(5, TimeUnit.SECONDS)).isTrue();
        startTogether.countDown();
        CompletableFuture.allOf(first, second).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            quotaWorker.applyPendingQuotaTasks();
            assertThat(jdbc.queryForObject("select count(*) from usage_quota_task where reservation_id=? and action_type='COMPENSATE'", Integer.class, reservationId))
                    .isEqualTo(1);
            assertThat(jdbc.queryForObject("select status from usage_quota_task where reservation_id=?", String.class, reservationId))
                    .isEqualTo("APPLIED");
            assertThat(redisCounter(quotaKey, "reserved")).isZero();
            assertThat(redisCounter(quotaKey, "active")).isZero();
        });
        System.out.printf("Racing recovery result: scans=2, compensationTasks=1, redisReserved=0, redisActive=0%n");
    }

    /** Waits for its peer so two Worker recovery scans query and update the same expired reservation concurrently. */
    private void runRecoveryScan(CountDownLatch clientsReady, CountDownLatch startTogether) {
        clientsReady.countDown();
        try {
            if (!startTogether.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Concurrent recovery start was not released");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Concurrent recovery scan was interrupted", exception);
        }
        usageWorker.reconcileUsage();
    }
    /** Keeps a token hold for later accounting when an expired provider call has no durable provider ID. */
    @Test
    void releasesOnlyConcurrencyForExpiredStartedReservationWithoutProviderId() {
        String reservationId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        long reservedTokens = 200;
        jdbc.update("""
                insert into usage_reservation(reservation_id,request_id,correlation_id,project_id,idempotency_key,
                    reserved_tokens,status,provider_started,expires_at,created_at,updated_at)
                values(?,?,?,?,?,?,'RESERVED',true,?,?,?)
                """, reservationId, UUID.randomUUID().toString(), reservationId, "project-fund-agent",
                "missing-provider-id-" + reservationId, reservedTokens, now.minusSeconds(1), now.minusSeconds(2), now.minusSeconds(2));
        String quotaKey = "agentops:quota:project-fund-agent";
        String reservationKey = "agentops:reservation:" + reservationId;
        redis.opsForHash().put(quotaKey, "reserved", Long.toString(reservedTokens));
        redis.opsForHash().put(quotaKey, "active", "1");
        redis.opsForHash().put(quotaKey, "consumed", "0");
        redis.opsForHash().put(reservationKey, "state", "RESERVED");
        redis.opsForHash().put(reservationKey, "tokens", Long.toString(reservedTokens));

        usageWorker.reconcileUsage();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            quotaWorker.applyPendingQuotaTasks();
            assertThat(jdbc.queryForObject("select status from usage_reservation where reservation_id=?", String.class, reservationId))
                    .isEqualTo("SETTLEMENT_PENDING");
            assertThat(jdbc.queryForObject("select settlement_status from usage_reservation where reservation_id=?", String.class, reservationId))
                    .isEqualTo("PENDING");
            assertThat(jdbc.queryForObject("select failure_code from usage_reservation where reservation_id=?", String.class, reservationId))
                    .isEqualTo("ID_UNAVAILABLE");
            assertThat(jdbc.queryForObject("select action_type from usage_quota_task where reservation_id=?", String.class, reservationId))
                    .isEqualTo("RELEASE_ACTIVE");
            assertThat(jdbc.queryForObject("select status from usage_quota_task where reservation_id=?", String.class, reservationId))
                    .isEqualTo("APPLIED");
            assertThat(redisCounter(quotaKey, "reserved")).isEqualTo(reservedTokens);
            assertThat(redisCounter(quotaKey, "active")).isZero();
            assertThat(redis.opsForHash().get(reservationKey, "state")).isEqualTo("AWAITING_USAGE");
        });
        System.out.printf("Missing provider ID result: reservedTokens=%d, settlement=PENDING, redisReserved=%d, redisActive=0%n",
                reservedTokens, reservedTokens);
    }
    /** Reclaims an expired quota lease and advances its fencing version exactly once. */
    @Test
    void reclaimsExpiredQuotaLeaseWithNewFence() {
        String reservationId = UUID.randomUUID().toString();
        String taskId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        long reservedTokens = 200;
        jdbc.update("""
                insert into usage_reservation(reservation_id,request_id,correlation_id,project_id,idempotency_key,
                    reserved_tokens,status,expires_at,created_at,updated_at)
                values(?,?,?,?,?,?,'RESERVED',?,?,?)
                """, reservationId, UUID.randomUUID().toString(), reservationId, "project-fund-agent",
                "quota-lease-" + reservationId, reservedTokens, now.plusSeconds(30), now, now);
        jdbc.update("""
                insert into usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,
                    next_attempt_at,lease_owner,lease_until,lease_version,created_at,updated_at)
                values(?,?,?,?,?,'PROCESSING',?,?,?,?,?,?)
                """, taskId, reservationId, UUID.randomUUID().toString(), "FINALIZE", 40,
                now.minusSeconds(30), "stale-worker", now.minusSeconds(1), 7, now.minusSeconds(60), now.minusSeconds(60));
        String quotaKey = "agentops:quota:project-fund-agent";
        String reservationKey = "agentops:reservation:" + reservationId;
        redis.opsForHash().put(quotaKey, "reserved", Long.toString(reservedTokens));
        redis.opsForHash().put(quotaKey, "active", "1");
        redis.opsForHash().put(quotaKey, "consumed", "0");
        redis.opsForHash().put(reservationKey, "state", "RESERVED");
        redis.opsForHash().put(reservationKey, "tokens", Long.toString(reservedTokens));

        quotaWorker.applyPendingQuotaTasks();

        var task = jdbc.queryForMap("select status,lease_owner,lease_until,lease_version from usage_quota_task where task_id=?", taskId);
        assertThat(task.get("status")).isEqualTo("APPLIED");
        assertThat(task.get("lease_owner")).isNull();
        assertThat(task.get("lease_until")).isNull();
        assertThat(((Number) task.get("lease_version")).longValue()).isEqualTo(8);
        assertThat(redisCounter(quotaKey, "reserved")).isZero();
        assertThat(redisCounter(quotaKey, "active")).isZero();
        assertThat(redisCounter(quotaKey, "consumed")).isEqualTo(40);
    }

    /** Reads a Redis quota field and treats a missing hash field as zero. */
    private long redisCounter(String key, String field) {
        Object value = redis.opsForHash().get(key, field);
        return value == null ? 0 : Long.parseLong(value.toString());
    }
}
