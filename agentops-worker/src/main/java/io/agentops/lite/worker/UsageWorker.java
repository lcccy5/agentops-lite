package io.agentops.lite.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.UsageLedgerEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Relays immutable usage facts, applies idempotent projections and reports recoverable discrepancies. */
@Component
public final class UsageWorker {
    private static final DefaultRedisScript<Long> COMPENSATE = compensationScript();
    /** Matches Server in-flight stub TTL so a COMPENSATED claim survives retries after a missing stub. */
    private static final String COMPENSATION_MARKER_TTL_MS = Long.toString(Duration.ofDays(7).toMillis());
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final KafkaTemplate<String, String> kafka;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    /** Creates the usage background worker. */
    public UsageWorker(JdbcTemplate jdbc, TransactionTemplate transactions, KafkaTemplate<String, String> kafka,
                       StringRedisTemplate redis, ObjectMapper mapper) {
        this.jdbc = jdbc; this.transactions = transactions; this.kafka = kafka; this.redis = redis; this.mapper = mapper;
    }

    /** Publishes pending outbox rows and marks only acknowledged records as published. */
    @Scheduled(fixedDelayString = "${agentops.worker.relay-delay-ms:1000}")
    public void relayUsageOutbox() {
        List<Map<String, Object>> rows = jdbc.queryForList("select event_id,event_key,payload_json from usage_outbox where status='PENDING' and next_attempt_at<=? order by created_at limit 100", Instant.now());
        for (Map<String, Object> row : rows) {
            String eventId = row.get("event_id").toString();
            try {
                kafka.send("agentops.usage.ledger.v1", row.get("event_key").toString(), row.get("payload_json").toString()).get();
                jdbc.update("update usage_outbox set status='PUBLISHED',published_at=? where event_id=? and status='PENDING'", Instant.now(), eventId);
            } catch (Exception exception) {
                jdbc.update("update usage_outbox set attempts=attempts+1,next_attempt_at=? where event_id=?", Instant.now().plusSeconds(5), eventId);
            }
        }
    }

    /**
     * Applies each ledger ID once even when Kafka redelivers a record.
     * Listener concurrency is capped operationally by the usage topic partition count.
     */
    @KafkaListener(topics = "agentops.usage.ledger.v1", concurrency = "${agentops.worker.usage-consumer-concurrency:4}")
    public void applyUsageProjection(String payload) throws Exception {
        UsageLedgerEvent event = mapper.readValue(payload, UsageLedgerEvent.class);
        transactions.executeWithoutResult(status -> {
            try { jdbc.update("insert into usage_projection_applied(ledger_id,applied_at) values(?,?)", event.ledgerId(), Instant.now()); }
            catch (DuplicateKeyException duplicate) { return; }
            int changed = jdbc.update("update usage_projection set total_tokens=total_tokens+?,total_cost=total_cost+?,updated_at=? where project_id=?",
                    event.tokenDelta(), event.costDelta(), Instant.now(), event.projectId());
            if (changed == 0) jdbc.update("insert into usage_projection(project_id,total_tokens,total_cost,updated_at) values(?,?,?,?)",
                    event.projectId(), event.tokenDelta(), event.costDelta(), Instant.now());
        });
    }

    /**
     * Expires unfinished reservations and reports ledger/projection/consumed divergence.
     * Redis holds are released from MySQL facts: RESERVED refunds without a stub, PENDING still requires one.
     */
    @Scheduled(fixedDelayString = "${agentops.worker.recovery-delay-ms:10000}")
    public void reconcileUsage() {
        Instant now = Instant.now();
        for (Map<String, Object> expired : jdbc.queryForList("select reservation_id,project_id,status,provider_started,reserved_tokens from usage_reservation where status in ('PENDING','RESERVED') and expires_at<? limit 200", now)) {
            String reservationId = expired.get("reservation_id").toString();
            Long released = compensateExpiredHold(expired);
            boolean providerStarted = Boolean.TRUE.equals(expired.get("provider_started"));
            String nextStatus = providerStarted ? "RECONCILIATION_PENDING" : "CANCELLED";
            String code = released != null && released == 1L ? "RESERVATION_EXPIRED_COMPENSATED" : "RESERVATION_EXPIRED_MARKER_MISSING";
            jdbc.update("update usage_reservation set status=?,failure_code=?,updated_at=? where reservation_id=? and status in ('PENDING','RESERVED')", nextStatus, code, now, reservationId);
        }
        for (Map<String, Object> project : jdbc.queryForList("select project_id from agent_project")) {
            String projectId = project.get("project_id").toString();
            Long ledger = jdbc.queryForObject("select coalesce(sum(token_delta),0) from usage_ledger where project_id=?", Long.class, projectId);
            List<Long> projected = jdbc.query("select total_tokens from usage_projection where project_id=?", (rs, row) -> rs.getLong(1), projectId);
            long projection = projected.isEmpty() ? 0 : projected.getFirst(); long expected = ledger == null ? 0 : ledger;
            if (expected != projection && !alreadyOpen(projectId, "LEDGER_PROJECTION", expected, projection)) discrepancy(projectId, "LEDGER_PROJECTION", expected, projection, "Replay unpublished usage outbox records or rebuild projection from ledger");
            Object redisValue = redis.opsForHash().get("agentops:quota:" + projectId, "consumed");
            long consumed = redisValue == null ? 0 : Long.parseLong(redisValue.toString());
            if (consumed != expected && !alreadyOpen(projectId, "LEDGER_REDIS", expected, consumed)) discrepancy(projectId, "LEDGER_REDIS", expected, consumed, "Inspect expired markers, then reconcile Redis counters from immutable ledger");
        }
    }

    /**
     * Releases the Redis hold for one expired MySQL reservation.
     * A RESERVED row is proof Lua already incremented quota, so the stub is optional.
     * A PENDING row still needs the stub; otherwise this would refund a crash that never reached Redis.
     *
     * @param expired reservation row containing reservation_id, project_id, status and reserved_tokens
     * @return 1 when quota and concurrency were released, or 0 when this hold was already gone or never taken
     */
    private Long compensateExpiredHold(Map<String, Object> expired) {
        String reservationId = expired.get("reservation_id").toString();
        String projectId = expired.get("project_id").toString();
        String tokens = Long.toString(((Number) expired.get("reserved_tokens")).longValue());
        String allowWithoutStub = "RESERVED".equals(String.valueOf(expired.get("status"))) ? "1" : "0";
        return redis.execute(COMPENSATE, List.of("agentops:quota:" + projectId, "agentops:reservation:" + reservationId),
                COMPENSATION_MARKER_TTL_MS, tokens, allowWithoutStub);
    }

    private boolean alreadyOpen(String project, String type, long expected, long actual) {
        Integer count = jdbc.queryForObject("select count(*) from usage_reconciliation where project_id=? and discrepancy_type=? and expected_value=? and actual_value=? and detected_at>?", Integer.class, project, type, expected, actual, Instant.now().minusSeconds(60));
        return count != null && count > 0;
    }
    private void discrepancy(String project, String type, long expected, long actual, String suggestion) {
        jdbc.update("insert into usage_reconciliation(reconciliation_id,project_id,discrepancy_type,expected_value,actual_value,suggested_action,detected_at) values(?,?,?,?,?,?,?)", UUID.randomUUID().toString(), project, type, expected, actual, suggestion, Instant.now());
    }
    private static DefaultRedisScript<Long> compensationScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(); script.setLocation(new ClassPathResource("lua/compensate.lua")); script.setResultType(Long.class); return script;
    }
}
