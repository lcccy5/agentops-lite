package io.agentops.lite.server.usage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.UsageLedgerEvent;
import io.agentops.lite.core.domain.TokenEstimator;
import io.agentops.lite.core.domain.UsageModels.ConfirmedUsage;
import io.agentops.lite.core.domain.UsageModels.Reservation;
import io.agentops.lite.core.domain.UsageModels.ReservationStatus;
import io.agentops.lite.server.config.AgentOpsProperties;
import io.agentops.lite.server.gateway.GatewayException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Owns MySQL-first admission, Redis quota reservation, immutable settlement and outbox creation. */
@Service
public final class UsageService {
    private static final DefaultRedisScript<List> RESERVE = script("lua/reserve.lua", List.class);
    private static final DefaultRedisScript<Long> FINALIZE = script("lua/finalize.lua", Long.class);
    private static final DefaultRedisScript<Long> COMPENSATE = script("lua/compensate.lua", Long.class);
    /** Keeps the PENDING-after-Lua stub alive across Worker downtime; expires_at stays the short admission timeout. */
    private static final String IN_FLIGHT_MARKER_TTL_MS = Long.toString(Duration.ofDays(7).toMillis());
    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final TransactionTemplate transactions;
    private final ObjectMapper mapper;
    private final AgentOpsProperties properties;

    /** Creates the state-machine service over MySQL and Redis. */
    public UsageService(JdbcTemplate jdbc, StringRedisTemplate redis, TransactionTemplate transactions,
                        ObjectMapper mapper, AgentOpsProperties properties) {
        this.jdbc = jdbc; this.redis = redis; this.transactions = transactions; this.mapper = mapper; this.properties = properties;
    }

    /**
     * Establishes the database fact before atomically reserving tokens and concurrency in Redis.
     * The in-flight stub TTL is longer than expires_at so Worker can still adjudicate PENDING-after-Lua crashes.
     */
    public Reservation reserve(String projectId, String requestId, String correlationId, String idempotencyKey,
                               com.fasterxml.jackson.databind.JsonNode request) {
        // Project limits are the admission contract; global values remain only deployment defaults for older projects.
        Map<String, Object> limits = jdbc.queryForMap("select token_limit,max_concurrency,default_max_tokens,project_max_tokens from agent_project where project_id=?", projectId);
        long tokens = TokenEstimator.reserve(request, Math.toIntExact(number(limits.get("default_max_tokens"))), Math.toIntExact(number(limits.get("project_max_tokens"))), properties.safetyMarginTokens());
        String reservationId = UUID.randomUUID().toString(); Instant now = Instant.now(); Instant expires = now.plus(properties.reservationTimeout());
        try {
            jdbc.update("insert into usage_reservation(reservation_id,request_id,correlation_id,project_id,idempotency_key,reserved_tokens,status,expires_at,created_at,updated_at) values(?,?,?,?,?,?,'PENDING',?,?,?)",
                    reservationId, requestId, correlationId, projectId, idempotencyKey, tokens, expires, now, now);
        } catch (DuplicateKeyException duplicate) {
            Reservation existing = findByIdempotency(projectId, idempotencyKey);
            throw new GatewayException(existing.status() == ReservationStatus.PENDING || existing.status() == ReservationStatus.RESERVED ? "REQUEST_IN_PROGRESS" : "REQUEST_ALREADY_FINALIZED",
                    HttpStatus.CONFLICT, "Idempotency key already exists with status " + existing.status());
        }
        String quotaKey = quotaKey(projectId); String markerKey = markerKey(reservationId);
        List<?> result;
        try {
            result = redis.execute(RESERVE, List.of(quotaKey, markerKey), limits.get("token_limit").toString(), limits.get("max_concurrency").toString(), Long.toString(tokens), IN_FLIGHT_MARKER_TTL_MS);
        } catch (RuntimeException exception) {
            reject(reservationId, "REDIS_UNAVAILABLE");
            throw new GatewayException("ADMISSION_UNAVAILABLE", HttpStatus.SERVICE_UNAVAILABLE, "Quota service is unavailable");
        }
        long outcome = ((Number) result.getFirst()).longValue();
        if (outcome != 1 && outcome != 2) {
            reject(reservationId, outcome == 0 && ((Number) result.get(1)).longValue() == -1 ? "CONCURRENCY_LIMIT" : "TOKEN_LIMIT");
            throw new GatewayException("QUOTA_REJECTED", HttpStatus.TOO_MANY_REQUESTS, "Project token or concurrency limit reached");
        }
        int confirmed = jdbc.update("update usage_reservation set status='RESERVED',updated_at=? where reservation_id=? and status='PENDING'", Instant.now(), reservationId);
        if (confirmed != 1) {
            // Confirm failed while still PENDING: only refund if the stub proves Lua already ran.
            redis.execute(COMPENSATE, List.of(quotaKey, markerKey), IN_FLIGHT_MARKER_TTL_MS);
            throw new GatewayException("RESERVATION_CONFIRM_FAILED", HttpStatus.SERVICE_UNAVAILABLE, "Reservation confirmation failed");
        }
        return new Reservation(reservationId, requestId, projectId, idempotencyKey, tokens, ReservationStatus.RESERVED, expires);
    }

    /** Preserves the original single-request contract for callers without an upstream run identifier. */
    public Reservation reserve(String projectId, String requestId, String idempotencyKey,
                               com.fasterxml.jackson.databind.JsonNode request) {
        return reserve(projectId, requestId, requestId, idempotencyKey, request);
    }

    /** Marks that upstream processing started so failures cannot be mistaken for unused requests. */
    public void markProviderStarted(String reservationId) {
        jdbc.update("update usage_reservation set provider_started=true,updated_at=? where reservation_id=?", Instant.now(), reservationId);
    }

    /** Appends an immutable ledger and outbox event exactly once, then releases the Redis permit. */
    public void finalizeReservation(Reservation reservation, ConfirmedUsage usage, String terminalStatus, String promptVersion) {
        // Provider usage is the settlement truth even when it exceeds the conservative reservation estimate.
        String ledgerId = UUID.randomUUID().toString(); long actual = Math.max(0, usage.totalTokens());
        Boolean written = transactions.execute(status -> {
            var states = jdbc.query("select status from usage_reservation where reservation_id=? for update", (rs, row) -> rs.getString(1), reservation.reservationId());
            if (states.isEmpty() || isFinal(states.getFirst())) return false;
            Instant now = Instant.now(); String type = usage.estimated() ? "USAGE_ESTIMATED" : "USAGE_ACTUAL";
            jdbc.update("insert into usage_ledger(ledger_id,reservation_id,project_id,ledger_type,token_delta,cost_delta,prompt_version,occurred_at) values(?,?,?,?,?,0,?,?)",
                    ledgerId, reservation.reservationId(), reservation.projectId(), type, actual, promptVersion, now);
            UsageLedgerEvent event = new UsageLedgerEvent(ledgerId, reservation.projectId(), reservation.reservationId(), type, actual, BigDecimal.ZERO, promptVersion, now);
            jdbc.update("insert into usage_outbox(event_id,ledger_id,event_key,payload_json,status,next_attempt_at,created_at) values(?,?,?,?, 'PENDING',?,?)",
                    UUID.randomUUID().toString(), ledgerId, ledgerId, json(event), now, now);
            String finalState = usage.estimated() ? "RECONCILIATION_PENDING" : terminalStatus;
            jdbc.update("update usage_reservation set actual_tokens=?,usage_source=?,status=?,prompt_version=?,updated_at=? where reservation_id=?",
                    actual, usage.estimated() ? "ESTIMATED" : "PROVIDER", finalState, promptVersion, now, reservation.reservationId());
            return true;
        });
        if (Boolean.TRUE.equals(written)) {
            Long released = redis.execute(FINALIZE, List.of(quotaKey(reservation.projectId()), markerKey(reservation.reservationId())), Long.toString(actual), "300000");
            if (released == null || released == 0L) jdbc.update("update usage_reservation set status='RECONCILIATION_PENDING',failure_code='REDIS_FINALIZE_FAILED',updated_at=? where reservation_id=?", Instant.now(), reservation.reservationId());
        }
    }

    /** Returns the current reservation view for diagnostics. */
    public Map<String, Object> queryRequest(String requestId) {
        return jdbc.queryForMap("select request_id,correlation_id,reservation_id,project_id,reserved_tokens,actual_tokens,status,usage_source,prompt_version,created_at,updated_at from usage_reservation where request_id=?", requestId);
    }

    /** Aggregates every provider call made by one upstream Agent run in chronological order. */
    public Map<String, Object> queryRun(String projectId, String correlationId) {
        List<Map<String, Object>> calls = jdbc.queryForList("""
                select r.request_id,r.reservation_id,r.reserved_tokens,r.actual_tokens,r.status,r.usage_source,
                       r.prompt_version,r.failure_code,r.created_at,r.updated_at,
                       coalesce(sum(l.token_delta),0) ledger_tokens,count(l.ledger_id) ledger_entries
                from usage_reservation r left join usage_ledger l on l.reservation_id=r.reservation_id
                where r.project_id=? and r.correlation_id=?
                group by r.request_id,r.reservation_id,r.reserved_tokens,r.actual_tokens,r.status,r.usage_source,
                         r.prompt_version,r.failure_code,r.created_at,r.updated_at
                order by r.created_at
                """, projectId, correlationId);
        long actualTokens = calls.stream().mapToLong(call -> number(call.get("actual_tokens"))).sum();
        long reservedTokens = calls.stream().mapToLong(call -> number(call.get("reserved_tokens"))).sum();
        boolean settled = !calls.isEmpty() && calls.stream().allMatch(call -> isFinal(String.valueOf(call.get("status"))));
        return Map.of("correlationId", correlationId, "modelCallCount", calls.size(), "reservedTokens", reservedTokens,
                "actualTokens", actualTokens, "settled", settled, "calls", calls);
    }

    /** Lists recent upstream Agent runs for the lightweight operator console. */
    public List<Map<String, Object>> queryRecentRuns(String projectId, int limit) {
        return jdbc.queryForList("""
                select correlation_id,count(*) model_call_count,sum(reserved_tokens) reserved_tokens,
                       sum(coalesce(actual_tokens,0)) actual_tokens,min(created_at) started_at,max(updated_at) updated_at,
                       case when sum(status in ('PENDING','RESERVED'))=0 then 'FINAL' else 'IN_PROGRESS' end settlement_status,
                       max(prompt_version) prompt_version
                from usage_reservation where project_id=? group by correlation_id
                order by max(created_at) desc limit ?
                """, projectId, Math.max(1, Math.min(limit, 200)));
    }

    /** Returns ledger and projection totals for the authenticated local project. */
    public Map<String, Object> querySummary(String projectId) {
        Long ledger = jdbc.queryForObject("select coalesce(sum(token_delta),0) from usage_ledger where project_id=?", Long.class, projectId);
        List<Long> projected = jdbc.query("select total_tokens from usage_projection where project_id=?", (rs, row) -> rs.getLong(1), projectId);
        return Map.of("projectId", projectId, "ledgerTokens", ledger == null ? 0 : ledger, "projectedTokens", projected.isEmpty() ? 0 : projected.getFirst());
    }

    /** Corrects one estimated settlement by appending an immutable adjustment and matching outbox event. */
    public Map<String, Object> adjustEstimatedUsage(String requestId, long correctedTokens) {
        if (correctedTokens < 0) throw new IllegalArgumentException("correctedTokens must be non-negative");
        return transactions.execute(status -> {
            Map<String, Object> reservation = jdbc.queryForMap("select reservation_id,project_id,actual_tokens,prompt_version,status from usage_reservation where request_id=? for update", requestId);
            if (!"RECONCILIATION_PENDING".equals(reservation.get("status"))) throw new GatewayException("USAGE_NOT_ADJUSTABLE", HttpStatus.CONFLICT, "Only estimated usage can be adjusted");
            String reservationId = reservation.get("reservation_id").toString(); String projectId = reservation.get("project_id").toString();
            Map<String, Object> original = jdbc.queryForMap("select ledger_id,token_delta from usage_ledger where reservation_id=? and ledger_type='USAGE_ESTIMATED' order by occurred_at limit 1", reservationId);
            long previous = ((Number) original.get("token_delta")).longValue(); long delta = correctedTokens - previous;
            String ledgerId = UUID.randomUUID().toString(); Instant now = Instant.now(); String prompt = (String) reservation.get("prompt_version");
            jdbc.update("insert into usage_ledger(ledger_id,reservation_id,project_id,ledger_type,related_ledger_id,token_delta,cost_delta,prompt_version,occurred_at) values(?,?,?,'USAGE_ADJUSTMENT',?,?,0,?,?)",
                    ledgerId, reservationId, projectId, original.get("ledger_id"), delta, prompt, now);
            UsageLedgerEvent event = new UsageLedgerEvent(ledgerId, projectId, reservationId, "USAGE_ADJUSTMENT", delta, BigDecimal.ZERO, prompt, now);
            jdbc.update("insert into usage_outbox(event_id,ledger_id,event_key,payload_json,status,next_attempt_at,created_at) values(?,?,?,?, 'PENDING',?,?)",
                    UUID.randomUUID().toString(), ledgerId, ledgerId, json(event), now, now);
            jdbc.update("update usage_reservation set actual_tokens=?,usage_source='ADJUSTED',status='SETTLED',updated_at=? where reservation_id=?", correctedTokens, now, reservationId);
            return Map.of("requestId", requestId, "relatedLedgerId", original.get("ledger_id"), "adjustmentLedgerId", ledgerId, "tokenDelta", delta, "correctedTokens", correctedTokens);
        });
    }

    private Reservation findByIdempotency(String projectId, String key) {
        return jdbc.queryForObject("select reservation_id,request_id,project_id,idempotency_key,reserved_tokens,status,expires_at from usage_reservation where project_id=? and idempotency_key=?",
                (rs, row) -> new Reservation(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getLong(5), ReservationStatus.valueOf(rs.getString(6)), rs.getTimestamp(7).toInstant()), projectId, key);
    }
    private void reject(String id, String code) { jdbc.update("update usage_reservation set status='REJECTED',failure_code=?,updated_at=? where reservation_id=?", code, Instant.now(), id); }
    private boolean isFinal(String status) { return !status.equals("PENDING") && !status.equals("RESERVED"); }
    private long number(Object value) { return value instanceof Number number ? number.longValue() : 0L; }
    private String json(Object value) { try { return mapper.writeValueAsString(value); } catch (JsonProcessingException exception) { throw new IllegalStateException(exception); } }
    private static String quotaKey(String projectId) { return "agentops:quota:" + projectId; }
    private static String markerKey(String id) { return "agentops:reservation:" + id; }
    private static <T> DefaultRedisScript<T> script(String path, Class<T> type) {
        DefaultRedisScript<T> script = new DefaultRedisScript<>(); script.setLocation(new ClassPathResource(path)); script.setResultType(type); return script;
    }
}
