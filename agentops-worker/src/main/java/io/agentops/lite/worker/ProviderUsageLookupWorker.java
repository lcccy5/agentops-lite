package io.agentops.lite.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.UsageLedgerEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.reactive.function.client.WebClient;

/** Resolves durable provider generation IDs into one authoritative usage settlement. */
@Component
public final class ProviderUsageLookupWorker {
    private static final DefaultRedisScript<Long> FINALIZE = script();
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final WebClient client;
    private final WorkerProperties properties;

    /** Creates the asynchronous query worker using the provider credential kept in worker configuration. */
    public ProviderUsageLookupWorker(JdbcTemplate jdbc, TransactionTemplate transactions, StringRedisTemplate redis,
                                     ObjectMapper mapper, WebClient evaluationWebClient, WorkerProperties properties) {
        this.jdbc = jdbc; this.transactions = transactions; this.redis = redis; this.mapper = mapper;
        this.client = evaluationWebClient; this.properties = properties;
    }

    /** Claims due lookup tasks, then performs provider I/O outside the database transaction. */
    @Scheduled(fixedDelayString = "${agentops.worker.recovery-delay-ms:10000}")
    public void resolveProviderUsage() {
        Instant now = Instant.now();
        for (Map<String, Object> task : jdbc.queryForList("select task_id,reservation_id,provider_generation_id,usage_query_path,provider_base_url,deadline_at,attempts from usage_lookup_task where (status='PENDING' and next_attempt_at<=?) or (status='PROCESSING' and lease_until<?) order by next_attempt_at limit 50", now, now)) {
            String taskId = task.get("task_id").toString();
            // A lease recovery may replay a request, so final settlement still checks PROCESSING atomically.
            if (jdbc.update("update usage_lookup_task set status='PROCESSING',lease_owner=?,lease_until=?,updated_at=? where task_id=? and (status='PENDING' or (status='PROCESSING' and lease_until<?))", "usage-lookup", now.plusSeconds(30), now, taskId, now) != 1) continue;
            try {
                JsonNode response = client.get().uri(task.get("provider_base_url") + path(task)).headers(headers -> headers.setBearerAuth(properties.providerApiKey()))
                        .retrieve().bodyToMono(JsonNode.class).block(Duration.ofSeconds(10));
                JsonNode usage = usageNode(response);
                if (usage == null || usage.isMissingNode() || usage.isNull()) throw new IllegalStateException("USAGE_NOT_READY");
                settleProviderUsage(task, usage.path("prompt_tokens").asLong(-1), usage.path("completion_tokens").asLong(-1));
            } catch (Exception error) {
                retryOrEstimate(task, error.getMessage());
            }
        }
    }

    /** Builds a configured lookup path and permits only the captured generation identifier substitution. */
    private String path(Map<String, Object> task) {
        return task.get("usage_query_path").toString().replace("{id}", java.net.URLEncoder.encode(task.get("provider_generation_id").toString(), java.nio.charset.StandardCharsets.UTF_8));
    }

    /** Accepts direct and common envelope response shapes while keeping endpoint choice configuration-driven. */
    private JsonNode usageNode(JsonNode response) {
        if (response == null) return null;
        JsonNode direct = response.path("usage");
        return direct.isMissingNode() || direct.isNull() ? response.path("data").path("usage") : direct;
    }
    /** Appends authoritative usage once and marks the lookup complete before applying Redis quota. */
    private void settleProviderUsage(Map<String, Object> task, long input, long output) {
        if (input < 0 || output < 0) throw new IllegalArgumentException("Provider usage must contain non-negative token counts");
        String reservationId = task.get("reservation_id").toString(); long total = input + output;
        Boolean settled = transactions.execute(status -> {
            Map<String, Object> reservation = jdbc.queryForMap("select project_id,prompt_version,execution_outcome from usage_reservation where reservation_id=? for update", reservationId);
            int changed = jdbc.update("update usage_lookup_task set status='COMPLETED',updated_at=? where task_id=? and status='PROCESSING'", Instant.now(), task.get("task_id"));
            if (changed != 1) return false;
            String ledgerId = UUID.randomUUID().toString(); Instant now = Instant.now();
            jdbc.update("insert into usage_ledger(ledger_id,reservation_id,project_id,ledger_type,token_delta,cost_delta,prompt_version,occurred_at) values(?,?,?,'USAGE_ACTUAL',?,0,?,?)", ledgerId, reservationId, reservation.get("project_id"), total, reservation.get("prompt_version"), now);
            UsageLedgerEvent event = new UsageLedgerEvent(ledgerId, reservation.get("project_id").toString(), reservationId, "USAGE_ACTUAL", total, BigDecimal.ZERO, (String) reservation.get("prompt_version"), now);
            jdbc.update("insert into usage_outbox(event_id,ledger_id,event_key,payload_json,status,next_attempt_at,created_at) values(?,?,?,?, 'PENDING',?,?)", UUID.randomUUID().toString(), ledgerId, ledgerId, json(event), now, now);
            jdbc.update("insert into usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,next_attempt_at,created_at,updated_at) values(?,?,?,?,?,'PENDING',?,?,?)", UUID.randomUUID().toString(), reservationId, ledgerId, "FINALIZE", total, now, now, now);
            jdbc.update("update usage_reservation set actual_tokens=?,input_tokens=?,output_tokens=?,usage_source='PROVIDER_QUERY',status=?,settlement_status='FINAL',updated_at=? where reservation_id=?", total, input, output, reservation.get("execution_outcome"), now, reservationId);
            return true;
        });
        if (Boolean.TRUE.equals(settled)) finalizeQuota(task, total);
    }

    /** Retries a transient lookup until its deadline, then settles the persisted estimate exactly once. */
    private void retryOrEstimate(Map<String, Object> task, String error) {
        Instant now = Instant.now(); Instant deadline = ((java.sql.Timestamp) task.get("deadline_at")).toInstant();
        int attempts = ((Number) task.get("attempts")).intValue() + 1;
        if (now.isBefore(deadline) && attempts <= properties.usageQueryRetryLimit()) {
            jdbc.update("update usage_lookup_task set status='PENDING',attempts=?,next_attempt_at=?,last_error_code=?,updated_at=? where task_id=? and status='PROCESSING'", attempts, now.plusSeconds(Math.min(60, 1L << Math.min(6, attempts))), safeError(error), now, task.get("task_id"));
            return;
        }
        Long estimate = jdbc.queryForObject("select actual_tokens from usage_reservation where reservation_id=?", Long.class, task.get("reservation_id"));
        settleEstimatedUsage(task, estimate == null ? 0 : estimate, safeError(error));
    }

    /** Converts the pre-persisted fallback estimate into a final, auditable ledger entry after expiry. */
    private void settleEstimatedUsage(Map<String, Object> task, long estimate, String reason) {
        String reservationId = task.get("reservation_id").toString();
        Boolean settled = transactions.execute(status -> {
            Map<String, Object> reservation = jdbc.queryForMap("select project_id,prompt_version,execution_outcome from usage_reservation where reservation_id=? for update", reservationId);
            if (jdbc.update("update usage_lookup_task set status='EXPIRED',last_error_code=?,updated_at=? where task_id=? and status='PROCESSING'", reason, Instant.now(), task.get("task_id")) != 1) return false;
            String ledgerId = UUID.randomUUID().toString(); Instant now = Instant.now();
            jdbc.update("insert into usage_ledger(ledger_id,reservation_id,project_id,ledger_type,token_delta,cost_delta,prompt_version,occurred_at) values(?,?,?,'USAGE_ESTIMATED',?,0,?,?)", ledgerId, reservationId, reservation.get("project_id"), estimate, reservation.get("prompt_version"), now);
            UsageLedgerEvent event = new UsageLedgerEvent(ledgerId, reservation.get("project_id").toString(), reservationId, "USAGE_ESTIMATED", estimate, BigDecimal.ZERO, (String) reservation.get("prompt_version"), now);
            jdbc.update("insert into usage_outbox(event_id,ledger_id,event_key,payload_json,status,next_attempt_at,created_at) values(?,?,?,?, 'PENDING',?,?)", UUID.randomUUID().toString(), ledgerId, ledgerId, json(event), now, now);
            jdbc.update("insert into usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,next_attempt_at,created_at,updated_at) values(?,?,?,?,?,'PENDING',?,?,?)", UUID.randomUUID().toString(), reservationId, ledgerId, "FINALIZE", estimate, now, now, now);
            jdbc.update("update usage_reservation set usage_source='ESTIMATED',status=?,settlement_status='FINAL',failure_code=?,updated_at=? where reservation_id=?", reservation.get("execution_outcome"), reason, now, reservationId);
            return true;
        });
        if (Boolean.TRUE.equals(settled)) finalizeQuota(task, estimate);
    }

    /** Applies the held quota release after the immutable ledger transaction has committed. */
    private void finalizeQuota(Map<String, Object> task, long total) {
        String projectId = jdbc.queryForObject("select project_id from usage_reservation where reservation_id=?", String.class, task.get("reservation_id"));
        redis.execute(FINALIZE, List.of("agentops:quota:" + projectId, "agentops:reservation:" + task.get("reservation_id")), Long.toString(total), "300000");
    }

    private String json(Object value) { try { return mapper.writeValueAsString(value); } catch (Exception exception) { throw new IllegalStateException(exception); } }
    private String safeError(String value) { return value == null ? "LOOKUP_FAILED" : value.substring(0, Math.min(value.length(), 120)); }
    private static DefaultRedisScript<Long> script() { DefaultRedisScript<Long> script = new DefaultRedisScript<>(); script.setLocation(new ClassPathResource("lua/finalize.lua")); script.setResultType(Long.class); return script; }
}
