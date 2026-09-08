package io.agentops.lite.worker;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Replays durable MySQL quota operations into the disposable Redis online projection. */
@Component
public final class UsageQuotaWorker {
    /** Finalizes held tokens after authoritative or estimated settlement. */
    private static final DefaultRedisScript<Long> FINALIZE = script("lua/finalize.lua");
    /** Releases only the connection slot while provider usage remains pending. */
    private static final DefaultRedisScript<Long> RELEASE_ACTIVE = script("lua/release_active.lua");
    /** Compensates an admission that expired before provider execution started. */
    private static final DefaultRedisScript<Long> COMPENSATE = script("lua/compensate.lua");
    /** Applies an immutable ledger adjustment once per operation marker. */
    private static final DefaultRedisScript<Long> ADJUST = script("lua/adjust_consumed.lua");
    /** Database fact store used to claim and finish retry tasks. */
    private final JdbcTemplate jdbc;
    /** Redis projection client used only after a task has been claimed. */
    private final StringRedisTemplate redis;

    /** Creates the retry worker over the shared usage database and Redis instance. */
    public UsageQuotaWorker(JdbcTemplate jdbc, StringRedisTemplate redis) { this.jdbc = jdbc; this.redis = redis; }

    /** Applies due operations; Lua marker state makes retries safe after ambiguous network failures. */
    @Scheduled(fixedDelayString = "${agentops.worker.recovery-delay-ms:10000}")
    public void applyPendingQuotaTasks() {
        for (Map<String, Object> task : jdbc.queryForList("select q.task_id,q.reservation_id,q.operation_id,q.action_type,q.token_value,r.project_id from usage_quota_task q join usage_reservation r on r.reservation_id=q.reservation_id where q.status='PENDING' and q.next_attempt_at<=? order by q.created_at limit 100", Instant.now())) {
            if (jdbc.update("update usage_quota_task set status='PROCESSING',updated_at=? where task_id=? and status='PENDING'", Instant.now(), task.get("task_id")) != 1) continue;
            try {
                apply(task);
                jdbc.update("update usage_quota_task set status='APPLIED',updated_at=? where task_id=? and status='PROCESSING'", Instant.now(), task.get("task_id"));
                jdbc.update("update usage_reservation set quota_sync_status='APPLIED',updated_at=? where reservation_id=?", Instant.now(), task.get("reservation_id"));
            } catch (RuntimeException error) {
                jdbc.update("update usage_quota_task set status='PENDING',attempts=attempts+1,next_attempt_at=?,last_error_code=?,updated_at=? where task_id=? and status='PROCESSING'", Instant.now().plusSeconds(5), safe(error), Instant.now(), task.get("task_id"));
                jdbc.update("update usage_reservation set quota_sync_status='FAILED',updated_at=? where reservation_id=?", Instant.now(), task.get("reservation_id"));
            }
        }
    }

    /** Selects the action-specific atomic Lua transition and rejects unknown database values. */
    private void apply(Map<String, Object> task) {
        String project = "agentops:quota:" + task.get("project_id");
        String reservation = "agentops:reservation:" + task.get("reservation_id");
        String value = task.get("token_value").toString();
        Long result = switch (task.get("action_type").toString()) {
            case "FINALIZE" -> redis.execute(FINALIZE, List.of(project, reservation), value, "604800000");
            case "RELEASE_ACTIVE" -> redis.execute(RELEASE_ACTIVE, List.of(project, reservation), "604800000");
            case "COMPENSATE" -> redis.execute(COMPENSATE, List.of(project, reservation), "604800000", value, "1");
            case "ADJUST" -> redis.execute(ADJUST, List.of(project, "agentops:quota-operation:" + task.get("operation_id")), value, "604800000");
            default -> throw new IllegalStateException("Unknown quota task action " + task.get("action_type"));
        };
        if (result == null || result == 0L) throw new IllegalStateException("Redis quota marker is unavailable");
    }

    /** Truncates retry diagnostics to the schema limit without persisting credentials or response bodies. */
    private String safe(RuntimeException error) {
        String value = error.getClass().getSimpleName() + ":" + String.valueOf(error.getMessage());
        return value.substring(0, Math.min(128, value.length()));
    }

    /** Loads one Lua operation from classpath with a numeric status result. */
    private static DefaultRedisScript<Long> script(String path) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(); script.setLocation(new ClassPathResource(path)); script.setResultType(Long.class); return script;
    }
}
