package io.agentops.lite.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.UsageLedgerEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Relays immutable usage facts, applies idempotent projections and reports recoverable discrepancies. */
@Component
public final class UsageWorker {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final KafkaTemplate<String, String> kafka;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final WorkerProperties properties;

    /** Creates the usage background worker. */
    public UsageWorker(JdbcTemplate jdbc, TransactionTemplate transactions, KafkaTemplate<String, String> kafka,
                       StringRedisTemplate redis, ObjectMapper mapper, WorkerProperties properties) {
        this.jdbc = jdbc; this.transactions = transactions; this.kafka = kafka; this.redis = redis; this.mapper = mapper;
        this.properties = properties;
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

        for (Map<String, Object> expired : jdbc.queryForList("""
                select r.reservation_id,r.project_id,r.status,r.provider_started,r.reserved_tokens,
                       a.attempt_id,a.provider_generation_id,p.settlement_mode,p.usage_query_path,p.base_url
                from usage_reservation r
                left join usage_provider_attempt a on a.reservation_id=r.reservation_id and a.attempt_no=1
                left join provider_config p on p.provider_id=a.provider_endpoint_id
                where r.status in ('PENDING','RESERVED') and r.expires_at<? limit 200
                """, now)) {
            String reservationId = expired.get("reservation_id").toString();
            boolean providerStarted = Boolean.TRUE.equals(expired.get("provider_started"));
            if (providerStarted) {
                if (!enqueueProviderRecovery(expired, now)) markMissingProviderId(expired, now);
                continue;
            }
            transactions.executeWithoutResult(status -> {
                if (jdbc.update("update usage_reservation set status='CANCELLED',settlement_status='FINAL',execution_outcome='CANCELLED',failure_code='RESERVATION_EXPIRED',updated_at=? where reservation_id=? and status in ('PENDING','RESERVED')", now, reservationId) != 1) return;
                jdbc.update("insert into usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,next_attempt_at,created_at,updated_at) values(?,?,?,?,?,'PENDING',?,?,?)",
                        UUID.randomUUID().toString(), reservationId, "expiry-compensate:" + reservationId, "COMPENSATE", expired.get("reserved_tokens"), now, now, now);
            });
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

    /** Releases only concurrency when provider execution started but no query identifier was captured. */
    private void markMissingProviderId(Map<String, Object> expired, Instant now) {
        transactions.executeWithoutResult(status -> {
            String reservationId = expired.get("reservation_id").toString();
            if (jdbc.update("update usage_reservation set status='SETTLEMENT_PENDING',settlement_status='PENDING',execution_outcome='UNKNOWN',failure_code='ID_UNAVAILABLE',updated_at=? where reservation_id=? and status in ('PENDING','RESERVED')", now, reservationId) != 1) return;
            jdbc.update("insert into usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,next_attempt_at,created_at,updated_at) values(?,?,?,?,?,'PENDING',?,?,?)",
                    UUID.randomUUID().toString(), reservationId, "missing-id-release-active:" + reservationId, "RELEASE_ACTIVE", 0, now, now, now);
        });
    }
    /** Converts an expired started call with durable provider evidence into one query and quota task. */
    private boolean enqueueProviderRecovery(Map<String, Object> expired, Instant now) {
        Object generationId = expired.get("provider_generation_id");
        Object queryPath = expired.get("usage_query_path");
        if (!"QUERYABLE".equals(String.valueOf(expired.get("settlement_mode"))) || generationId == null
                || queryPath == null || queryPath.toString().isBlank()) return false;
        Boolean queued = transactions.execute(status -> {
            String reservationId = expired.get("reservation_id").toString();
            Instant deadline = now.plus(properties.usageQueryDeadline());
            if (jdbc.update("update usage_reservation set status='SETTLEMENT_PENDING',settlement_status='PENDING',execution_outcome='UNKNOWN',settlement_deadline=?,failure_code='RECOVERED_AFTER_EXPIRY',updated_at=? where reservation_id=? and status in ('PENDING','RESERVED')", deadline, now, reservationId) != 1) return false;
            jdbc.update("insert into usage_lookup_task(task_id,reservation_id,attempt_id,provider_generation_id,usage_query_path,provider_base_url,status,next_attempt_at,deadline_at,created_at,updated_at) values(?,?,?,?,?,?,'PENDING',?,?,?,?)",
                    UUID.randomUUID().toString(), reservationId, expired.get("attempt_id"), generationId, queryPath, expired.get("base_url"), now, deadline, now, now);
            jdbc.update("insert into usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,next_attempt_at,created_at,updated_at) values(?,?,?,?,?,'PENDING',?,?,?)",
                    UUID.randomUUID().toString(), reservationId, "recovery-release-active:" + reservationId, "RELEASE_ACTIVE", 0, now, now, now);
            return true;
        });
        return Boolean.TRUE.equals(queued);
    }
    private boolean alreadyOpen(String project, String type, long expected, long actual) {
        Integer count = jdbc.queryForObject("select count(*) from usage_reconciliation where project_id=? and discrepancy_type=? and expected_value=? and actual_value=? and detected_at>?", Integer.class, project, type, expected, actual, Instant.now().minusSeconds(60));
        return count != null && count > 0;
    }
    private void discrepancy(String project, String type, long expected, long actual, String suggestion) {
        jdbc.update("insert into usage_reconciliation(reconciliation_id,project_id,discrepancy_type,expected_value,actual_value,suggested_action,detected_at) values(?,?,?,?,?,?,?)", UUID.randomUUID().toString(), project, type, expected, actual, suggestion, Instant.now());
    }
}
