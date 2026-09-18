package io.agentops.lite.server.usage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.UsageLedgerEvent;
import io.agentops.lite.core.domain.TokenEstimator;
import io.agentops.lite.core.domain.UsageModels.ConfirmedUsage;
import io.agentops.lite.core.domain.UsageModels.Reservation;
import io.agentops.lite.core.domain.UsageModels.ReservationStatus;
import io.agentops.lite.core.domain.UsageModels.SettlementMode;
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

/**
 * Owns MySQL-first admission, Redis quota reservation, immutable settlement and outbox creation.
 */
@Service
public final class UsageService {
  private static final String SELECT_PROJECT_LIMITS =
      """
      select token_limit,
             max_concurrency,
             default_max_tokens,
             project_max_tokens
      from agent_project
      where project_id = ?
      """;
  private static final String INSERT_PENDING_RESERVATION =
      """
      insert into usage_reservation (
          reservation_id,
          request_id,
          correlation_id,
          project_id,
          idempotency_key,
          reserved_tokens,
          status,
          expires_at,
          created_at,
          updated_at
      ) values (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
      """;
  private static final String CONFIRM_RESERVATION =
      """
      update usage_reservation
      set status = 'RESERVED', updated_at = ?
      where reservation_id = ? and status = 'PENDING'
      """;

  private static final DefaultRedisScript<List> RESERVE = script("lua/reserve.lua", List.class);
  private static final DefaultRedisScript<Long> FINALIZE = script("lua/finalize.lua", Long.class);
  private static final DefaultRedisScript<Long> RELEASE_ACTIVE =
      script("lua/release_active.lua", Long.class);
  private static final DefaultRedisScript<Long> COMPENSATE =
      script("lua/compensate.lua", Long.class);

  /**
   * Keeps the PENDING-after-Lua stub alive across Worker downtime; expires_at stays the short
   * admission timeout.
   */
  private static final String IN_FLIGHT_MARKER_TTL_MS =
      Long.toString(Duration.ofDays(7).toMillis());

  private final JdbcTemplate jdbc;
  private final StringRedisTemplate redis;
  private final TransactionTemplate transactions;
  private final ObjectMapper mapper;
  private final AgentOpsProperties properties;

  /** Creates the state-machine service over MySQL and Redis. */
  public UsageService(
      JdbcTemplate jdbc,
      StringRedisTemplate redis,
      TransactionTemplate transactions,
      ObjectMapper mapper,
      AgentOpsProperties properties) {
    this.jdbc = jdbc;
    this.redis = redis;
    this.transactions = transactions;
    this.mapper = mapper;
    this.properties = properties;
  }

  /**
   * Establishes the database fact before atomically reserving tokens and concurrency in Redis. The
   * in-flight stub TTL is longer than expires_at so Worker can still adjudicate PENDING-after-Lua
   * crashes.
   */
  public Reservation reserve(
      String projectId,
      String requestId,
      String correlationId,
      String idempotencyKey,
      JsonNode request) {
    ProjectLimits limits = findProjectLimits(projectId);
    long reservedTokens =
        TokenEstimator.reserve(
            request,
            limits.defaultMaxTokens(),
            limits.projectMaxTokens(),
            properties.safetyMarginTokens());

    Reservation pending =
        createPendingReservation(
            projectId, requestId, correlationId, idempotencyKey, reservedTokens, Instant.now());
    QuotaDecision decision = reserveQuota(pending, limits);
    rejectIfQuotaExceeded(pending, decision);
    confirmReservation(pending);

    return new Reservation(
        pending.reservationId(),
        pending.requestId(),
        pending.projectId(),
        pending.idempotencyKey(),
        pending.reservedTokens(),
        ReservationStatus.RESERVED,
        pending.expiresAt());
  }

  private ProjectLimits findProjectLimits(String projectId) {
    return jdbc.queryForObject(
        SELECT_PROJECT_LIMITS,
        (resultSet, rowNumber) ->
            new ProjectLimits(
                resultSet.getLong("token_limit"),
                resultSet.getInt("max_concurrency"),
                resultSet.getInt("default_max_tokens"),
                resultSet.getInt("project_max_tokens")),
        projectId);
  }

  private Reservation createPendingReservation(
      String projectId,
      String requestId,
      String correlationId,
      String idempotencyKey,
      long reservedTokens,
      Instant now) {
    String reservationId = UUID.randomUUID().toString();
    Instant expiresAt = now.plus(properties.reservationTimeout());
    try {
      jdbc.update(
          INSERT_PENDING_RESERVATION,
          reservationId,
          requestId,
          correlationId,
          projectId,
          idempotencyKey,
          reservedTokens,
          expiresAt,
          now,
          now);
    } catch (DuplicateKeyException duplicate) {
      throw duplicateReservation(projectId, idempotencyKey);
    }

    return new Reservation(
        reservationId,
        requestId,
        projectId,
        idempotencyKey,
        reservedTokens,
        ReservationStatus.PENDING,
        expiresAt);
  }

  private GatewayException duplicateReservation(String projectId, String idempotencyKey) {
    Reservation existing = findByIdempotency(projectId, idempotencyKey);
    boolean stillInProgress =
        existing.status() == ReservationStatus.PENDING
            || existing.status() == ReservationStatus.RESERVED;
    String errorCode = stillInProgress ? "REQUEST_IN_PROGRESS" : "REQUEST_ALREADY_FINALIZED";
    return new GatewayException(
        errorCode,
        HttpStatus.CONFLICT,
        "Idempotency key already exists with status " + existing.status());
  }

  private QuotaDecision reserveQuota(Reservation reservation, ProjectLimits limits) {
    List<?> result;
    try {
      result =
          redis.execute(
              RESERVE,
              List.of(quotaKey(reservation.projectId()), markerKey(reservation.reservationId())),
              Long.toString(limits.tokenLimit()),
              Integer.toString(limits.maxConcurrency()),
              Long.toString(reservation.reservedTokens()),
              IN_FLIGHT_MARKER_TTL_MS);
    } catch (RuntimeException exception) {
      reject(reservation.reservationId(), "REDIS_UNAVAILABLE");
      throw new GatewayException(
          "ADMISSION_UNAVAILABLE", HttpStatus.SERVICE_UNAVAILABLE, "Quota service is unavailable");
    }

    long outcome = ((Number) result.getFirst()).longValue();
    if (outcome == 1 || outcome == 2) {
      return QuotaDecision.acceptedDecision();
    }

    long detail = ((Number) result.get(1)).longValue();
    String rejectionCode = outcome == 0 && detail == -1 ? "CONCURRENCY_LIMIT" : "TOKEN_LIMIT";
    return QuotaDecision.rejectedDecision(rejectionCode);
  }

  private void rejectIfQuotaExceeded(Reservation reservation, QuotaDecision decision) {
    if (decision.accepted()) {
      return;
    }

    reject(reservation.reservationId(), decision.rejectionCode());
    throw new GatewayException(
        "QUOTA_REJECTED",
        HttpStatus.TOO_MANY_REQUESTS,
        "Project token or concurrency limit reached");
  }

  private void confirmReservation(Reservation reservation) {
    int confirmed = jdbc.update(CONFIRM_RESERVATION, Instant.now(), reservation.reservationId());
    if (confirmed != 1) {
      // Confirm failed while still PENDING: only refund if the stub proves Lua already ran.
      redis.execute(
          COMPENSATE,
          List.of(quotaKey(reservation.projectId()), markerKey(reservation.reservationId())),
          IN_FLIGHT_MARKER_TTL_MS);
      throw new GatewayException(
          "RESERVATION_CONFIRM_FAILED",
          HttpStatus.SERVICE_UNAVAILABLE,
          "Reservation confirmation failed");
    }
  }

  /**
   * Preserves the original single-request contract for callers without an upstream run identifier.
   */
  public Reservation reserve(
      String projectId, String requestId, String idempotencyKey, JsonNode request) {
    return reserve(projectId, requestId, requestId, idempotencyKey, request);
  }

  /** Marks that upstream processing started so failures cannot be mistaken for unused requests. */
  public void markProviderStarted(String reservationId) {
    jdbc.update(
        "update usage_reservation set provider_started=true,updated_at=? where reservation_id=?",
        Instant.now(),
        reservationId);
    // Persist an attempt before response headers or stream events can be lost on cancellation.
    jdbc.update(
        "insert into"
            + " usage_provider_attempt(attempt_id,reservation_id,attempt_no,provider_endpoint_id,requested_model,started_at,updated_at)"
            + " select ?,r.reservation_id,1,p.provider_id,p.model_name,?,? from usage_reservation r"
            + " join provider_config p on p.project_id=r.project_id and p.enabled=true where"
            + " r.reservation_id=? order by p.provider_id limit 1 on duplicate key update"
            + " updated_at=values(updated_at)",
        UUID.randomUUID().toString(),
        Instant.now(),
        Instant.now(),
        reservationId);
  }

  /** Records a provider-side request identifier as soon as the gateway observes it. */
  public void recordProviderGenerationId(String reservationId, String generationId) {
    if (generationId == null || generationId.isBlank()) return;
    jdbc.update(
        "update usage_provider_attempt set provider_generation_id=?,updated_at=? where"
            + " reservation_id=? and attempt_no=1",
        generationId,
        Instant.now(),
        reservationId);
  }

  /** Appends an immutable ledger and outbox event exactly once, then releases the Redis permit. */
  public void finalizeReservation(
      Reservation reservation, ConfirmedUsage usage, String terminalStatus, String promptVersion) {
    // Provider usage is the settlement truth even when it exceeds the conservative reservation
    // estimate.
    String ledgerId = UUID.randomUUID().toString();
    long actual = Math.max(0, usage.totalTokens());
    if (usage.estimated() && canQueryMissingUsage(reservation.projectId())) {
      Boolean queued =
          transactions.execute(
              status -> {
                var attempts =
                    jdbc.queryForList(
                        "select attempt_id,provider_generation_id from usage_provider_attempt where"
                            + " reservation_id=? and attempt_no=1",
                        reservation.reservationId());
                if (attempts.isEmpty() || attempts.getFirst().get("provider_generation_id") == null)
                  return false;
                var states =
                    jdbc.query(
                        "select status from usage_reservation where reservation_id=? for update",
                        (rs, row) -> rs.getString(1),
                        reservation.reservationId());
                if (states.isEmpty() || isFinal(states.getFirst())) return false;
                Instant now = Instant.now();
                Instant deadline = now.plus(properties.usageQueryDeadline());
                Map<String, Object> attempt = attempts.getFirst();
                // The unique reservation key makes repeated cancellation finalizers schedule one
                // lookup.
                jdbc.update(
                    "insert into"
                        + " usage_lookup_task(task_id,reservation_id,attempt_id,provider_generation_id,usage_query_path,provider_base_url,status,next_attempt_at,deadline_at,created_at,updated_at)"
                        + " values(?,?,?,?,?,?,'PENDING',?,?,?,?)",
                    UUID.randomUUID().toString(),
                    reservation.reservationId(),
                    attempt.get("attempt_id"),
                    attempt.get("provider_generation_id"),
                    usageQueryPath(reservation.projectId()),
                    settlementEndpoint(reservation.projectId()).baseUrl(),
                    now,
                    deadline,
                    now,
                    now);
                jdbc.update(
                    "insert into"
                        + " usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,next_attempt_at,created_at,updated_at)"
                        + " values(?,?,?,?,?,'PENDING',?,?,?)",
                    UUID.randomUUID().toString(),
                    reservation.reservationId(),
                    UUID.randomUUID().toString(),
                    "RELEASE_ACTIVE",
                    0,
                    now,
                    now,
                    now);
                jdbc.update(
                    "update usage_reservation set"
                        + " actual_tokens=?,input_tokens=?,output_tokens=?,usage_source='ESTIMATED_PENDING',estimator_version='heuristic-v1',status='SETTLEMENT_PENDING',settlement_status='PENDING',execution_outcome=?,settlement_deadline=?,prompt_version=?,updated_at=?"
                        + " where reservation_id=?",
                    actual,
                    usage.inputTokens(),
                    usage.outputTokens(),
                    terminalStatus,
                    deadline,
                    promptVersion,
                    now,
                    reservation.reservationId());
                return true;
              });
      if (Boolean.TRUE.equals(queued)) {
        Long released =
            redis.execute(
                RELEASE_ACTIVE,
                List.of(quotaKey(reservation.projectId()), markerKey(reservation.reservationId())),
                Long.toString(properties.usageQueryDeadline().toMillis()));
        if (released == null || released == 0L)
          jdbc.update(
              "update usage_reservation set"
                  + " quota_sync_status='FAILED',failure_code='REDIS_RELEASE_ACTIVE_FAILED',updated_at=?"
                  + " where reservation_id=?",
              Instant.now(),
              reservation.reservationId());
        return;
      }
    }
    Boolean written =
        transactions.execute(
            status -> {
              var states =
                  jdbc.query(
                      "select status from usage_reservation where reservation_id=? for update",
                      (rs, row) -> rs.getString(1),
                      reservation.reservationId());
              if (states.isEmpty() || isFinal(states.getFirst())) return false;
              Instant now = Instant.now();
              String type = usage.estimated() ? "USAGE_ESTIMATED" : "USAGE_ACTUAL";
              jdbc.update(
                  "insert into"
                      + " usage_ledger(ledger_id,reservation_id,project_id,ledger_type,token_delta,cost_delta,prompt_version,occurred_at)"
                      + " values(?,?,?,?,?,0,?,?)",
                  ledgerId,
                  reservation.reservationId(),
                  reservation.projectId(),
                  type,
                  actual,
                  promptVersion,
                  now);
              UsageLedgerEvent event =
                  new UsageLedgerEvent(
                      ledgerId,
                      reservation.projectId(),
                      reservation.reservationId(),
                      type,
                      actual,
                      BigDecimal.ZERO,
                      promptVersion,
                      now);
              jdbc.update(
                  "insert into"
                      + " usage_outbox(event_id,ledger_id,event_key,payload_json,status,next_attempt_at,created_at)"
                      + " values(?,?,?,?, 'PENDING',?,?)",
                  UUID.randomUUID().toString(),
                  ledgerId,
                  ledgerId,
                  json(event),
                  now,
                  now);
              jdbc.update(
                  "insert into"
                      + " usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,next_attempt_at,created_at,updated_at)"
                      + " values(?,?,?,?,?,'PENDING',?,?,?)",
                  UUID.randomUUID().toString(),
                  reservation.reservationId(),
                  ledgerId,
                  "FINALIZE",
                  actual,
                  now,
                  now,
                  now);
              jdbc.update(
                  "update usage_reservation set"
                      + " actual_tokens=?,input_tokens=?,output_tokens=?,usage_source=?,status=?,settlement_status='FINAL',execution_outcome=?,estimator_version=?,prompt_version=?,updated_at=?"
                      + " where reservation_id=?",
                  actual,
                  usage.inputTokens(),
                  usage.outputTokens(),
                  usage.estimated() ? "ESTIMATED" : "PROVIDER_STREAM",
                  terminalStatus,
                  terminalStatus,
                  usage.estimated() ? "heuristic-v1" : null,
                  promptVersion,
                  now,
                  reservation.reservationId());
              return true;
            });
    if (Boolean.TRUE.equals(written)) {
      Long released =
          redis.execute(
              FINALIZE,
              List.of(quotaKey(reservation.projectId()), markerKey(reservation.reservationId())),
              Long.toString(actual),
              "300000");
      if (released == null || released == 0L)
        jdbc.update(
            "update usage_reservation set"
                + " quota_sync_status='FAILED',failure_code='REDIS_FINALIZE_FAILED',updated_at=?"
                + " where reservation_id=?",
            Instant.now(),
            reservation.reservationId());
    }
  }

  /** Persists a just-observed provider ID before choosing queryable or estimated settlement. */
  public void finalizeReservation(
      Reservation reservation,
      ConfirmedUsage usage,
      String terminalStatus,
      String promptVersion,
      String providerGenerationId) {
    recordProviderGenerationId(reservation.reservationId(), providerGenerationId);
    finalizeReservation(reservation, usage, terminalStatus, promptVersion);
  }

  /** Returns true only when a configured endpoint can retrieve final request-level usage. */
  private boolean canQueryMissingUsage(String projectId) {
    ProviderEndpoint endpoint = settlementEndpoint(projectId);
    return SettlementMode.QUERYABLE.name().equalsIgnoreCase(endpoint.settlementMode())
        && endpoint.usageQueryPath() != null
        && !endpoint.usageQueryPath().isBlank();
  }

  /**
   * Returns the configured usage-query path for the exact project endpoint chosen for this request.
   */
  private String usageQueryPath(String projectId) {
    return settlementEndpoint(projectId).usageQueryPath();
  }

  /** Returns the configured inference origin from the deterministic settlement endpoint. */
  public String providerBaseUrl(String projectId) {
    return settlementEndpoint(projectId).baseUrl();
  }

  private ProviderEndpoint settlementEndpoint(String projectId) {
    List<ProviderEndpoint> rows =
        jdbc.query(
            """
            select provider_id, base_url, settlement_mode, usage_query_path
            from provider_config
            where project_id = ? and enabled = true
            order by provider_id
            limit 1
            """,
            (resultSet, rowNumber) ->
                new ProviderEndpoint(
                    resultSet.getString("provider_id"),
                    resultSet.getString("base_url"),
                    resultSet.getString("settlement_mode"),
                    resultSet.getString("usage_query_path")),
            projectId);
    if (rows.isEmpty())
      throw new GatewayException(
          "PROVIDER_NOT_CONFIGURED",
          HttpStatus.SERVICE_UNAVAILABLE,
          "No enabled provider endpoint is configured");
    return rows.getFirst();
  }

  /** Returns the current reservation view for diagnostics. */
  public Map<String, Object> queryRequest(String projectId, String requestId) {
    return jdbc.queryForMap(
        "select"
            + " request_id,correlation_id,reservation_id,project_id,reserved_tokens,actual_tokens,input_tokens,output_tokens,status,settlement_status,execution_outcome,usage_source,estimator_version,settlement_deadline,quota_sync_status,prompt_version,failure_code,created_at,updated_at"
            + " from usage_reservation where request_id=? and project_id=?",
        requestId,
        projectId);
  }

  /** Aggregates every provider call made by one upstream Agent run in chronological order. */
  public Map<String, Object> queryRun(String projectId, String correlationId) {
    List<Map<String, Object>> calls =
        jdbc.queryForList(
"""
select r.request_id,r.reservation_id,r.reserved_tokens,r.actual_tokens,r.status,r.settlement_status,r.execution_outcome,r.quota_sync_status,r.usage_source,
       r.prompt_version,r.failure_code,r.created_at,r.updated_at,
       coalesce(sum(l.token_delta),0) ledger_tokens,count(l.ledger_id) ledger_entries
from usage_reservation r left join usage_ledger l on l.reservation_id=r.reservation_id
where r.project_id=? and r.correlation_id=?
group by r.request_id,r.reservation_id,r.reserved_tokens,r.actual_tokens,r.status,r.settlement_status,r.execution_outcome,r.quota_sync_status,r.usage_source,
         r.prompt_version,r.failure_code,r.created_at,r.updated_at
order by r.created_at
""",
            projectId,
            correlationId);
    long actualTokens = calls.stream().mapToLong(call -> number(call.get("actual_tokens"))).sum();
    long reservedTokens =
        calls.stream().mapToLong(call -> number(call.get("reserved_tokens"))).sum();
    boolean settled =
        !calls.isEmpty()
            && calls.stream().allMatch(call -> "FINAL".equals(call.get("settlement_status")));
    return Map.of(
        "correlationId",
        correlationId,
        "modelCallCount",
        calls.size(),
        "reservedTokens",
        reservedTokens,
        "actualTokens",
        actualTokens,
        "settled",
        settled,
        "calls",
        calls);
  }

  /** Lists recent upstream Agent runs for the lightweight operator console. */
  public List<Map<String, Object>> queryRecentRuns(String projectId, int limit) {
    return jdbc.queryForList(
"""
select correlation_id,count(*) model_call_count,sum(reserved_tokens) reserved_tokens,
       sum(coalesce(actual_tokens,0)) actual_tokens,min(created_at) started_at,max(updated_at) updated_at,
       case when sum(settlement_status<>'FINAL')=0 then 'FINAL' else 'IN_PROGRESS' end settlement_status,
       max(prompt_version) prompt_version
from usage_reservation where project_id=? group by correlation_id
order by max(created_at) desc limit ?
""",
        projectId,
        Math.max(1, Math.min(limit, 200)));
  }

  /** Returns ledger and projection totals for the authenticated local project. */
  public Map<String, Object> querySummary(String projectId) {
    Long ledger =
        jdbc.queryForObject(
            "select coalesce(sum(token_delta),0) from usage_ledger where project_id=?",
            Long.class,
            projectId);
    List<Long> projected =
        jdbc.query(
            "select total_tokens from usage_projection where project_id=?",
            (rs, row) -> rs.getLong(1),
            projectId);
    return Map.of(
        "projectId",
        projectId,
        "ledgerTokens",
        ledger == null ? 0 : ledger,
        "projectedTokens",
        projected.isEmpty() ? 0 : projected.getFirst());
  }

  /**
   * Corrects one estimated settlement by appending an immutable adjustment and matching outbox
   * event.
   */
  public Map<String, Object> adjustEstimatedUsage(
      String projectId, String requestId, long correctedTokens) {
    if (correctedTokens < 0)
      throw new IllegalArgumentException("correctedTokens must be non-negative");
    return transactions.execute(
        status -> {
          Map<String, Object> reservation =
              jdbc.queryForMap(
                  "select"
                      + " reservation_id,project_id,actual_tokens,prompt_version,status,usage_source,settlement_status"
                      + " from usage_reservation where request_id=? and project_id=? for update",
                  requestId,
                  projectId);
          if (!"ESTIMATED".equals(reservation.get("usage_source"))
              || !"FINAL".equals(reservation.get("settlement_status")))
            throw new GatewayException(
                "USAGE_NOT_ADJUSTABLE",
                HttpStatus.CONFLICT,
                "Only final estimated usage can be adjusted");
          String reservationId = reservation.get("reservation_id").toString();
          String reservationProjectId = reservation.get("project_id").toString();
          Map<String, Object> original =
              jdbc.queryForMap(
                  "select ledger_id,token_delta from usage_ledger where reservation_id=? and"
                      + " ledger_type='USAGE_ESTIMATED' order by occurred_at limit 1",
                  reservationId);
          Long current =
              jdbc.queryForObject(
                  "select coalesce(sum(token_delta),0) from usage_ledger where reservation_id=?",
                  Long.class,
                  reservationId);
          long delta = correctedTokens - (current == null ? 0 : current);
          String ledgerId = UUID.randomUUID().toString();
          Instant now = Instant.now();
          String prompt = (String) reservation.get("prompt_version");
          jdbc.update(
              "insert into"
                  + " usage_ledger(ledger_id,reservation_id,project_id,ledger_type,related_ledger_id,token_delta,cost_delta,prompt_version,occurred_at)"
                  + " values(?,?,?,'USAGE_ADJUSTMENT',?,?,0,?,?)",
              ledgerId,
              reservationId,
              reservationProjectId,
              original.get("ledger_id"),
              delta,
              prompt,
              now);
          UsageLedgerEvent event =
              new UsageLedgerEvent(
                  ledgerId,
                  reservationProjectId,
                  reservationId,
                  "USAGE_ADJUSTMENT",
                  delta,
                  BigDecimal.ZERO,
                  prompt,
                  now);
          jdbc.update(
              "insert into"
                  + " usage_outbox(event_id,ledger_id,event_key,payload_json,status,next_attempt_at,created_at)"
                  + " values(?,?,?,?, 'PENDING',?,?)",
              UUID.randomUUID().toString(),
              ledgerId,
              ledgerId,
              json(event),
              now,
              now);
          jdbc.update(
              "insert into"
                  + " usage_quota_task(task_id,reservation_id,operation_id,action_type,token_value,status,next_attempt_at,created_at,updated_at)"
                  + " values(?,?,?,?,?,'PENDING',?,?,?)",
              UUID.randomUUID().toString(),
              reservationId,
              ledgerId,
              "ADJUST",
              delta,
              now,
              now,
              now);
          jdbc.update(
              "update usage_reservation set"
                  + " actual_tokens=?,usage_source='MANUAL',settlement_status='FINAL',updated_at=?"
                  + " where reservation_id=?",
              correctedTokens,
              now,
              reservationId);
          return Map.of(
              "requestId",
              requestId,
              "relatedLedgerId",
              original.get("ledger_id"),
              "adjustmentLedgerId",
              ledgerId,
              "tokenDelta",
              delta,
              "correctedTokens",
              correctedTokens);
        });
  }

  private Reservation findByIdempotency(String projectId, String key) {
    return jdbc.queryForObject(
        "select"
            + " reservation_id,request_id,project_id,idempotency_key,reserved_tokens,status,expires_at"
            + " from usage_reservation where project_id=? and idempotency_key=?",
        (rs, row) ->
            new Reservation(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3),
                rs.getString(4),
                rs.getLong(5),
                ReservationStatus.valueOf(rs.getString(6)),
                rs.getTimestamp(7).toInstant()),
        projectId,
        key);
  }

  private void reject(String id, String code) {
    jdbc.update(
        "update usage_reservation set status='REJECTED',failure_code=?,updated_at=? where"
            + " reservation_id=?",
        code,
        Instant.now(),
        id);
  }

  private boolean isFinal(String status) {
    return !status.equals("PENDING") && !status.equals("RESERVED");
  }

  private long number(Object value) {
    return value instanceof Number number ? number.longValue() : 0L;
  }

  private String json(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException(exception);
    }
  }

  private static String quotaKey(String projectId) {
    return "agentops:quota:" + projectId;
  }

  private static String markerKey(String id) {
    return "agentops:reservation:" + id;
  }

  private static <T> DefaultRedisScript<T> script(String path, Class<T> type) {
    DefaultRedisScript<T> script = new DefaultRedisScript<>();
    script.setLocation(new ClassPathResource(path));
    script.setResultType(type);
    return script;
  }

  private record ProjectLimits(
      long tokenLimit, int maxConcurrency, int defaultMaxTokens, int projectMaxTokens) {}

  private record ProviderEndpoint(
      String providerId, String baseUrl, String settlementMode, String usageQueryPath) {}

  private record QuotaDecision(boolean accepted, String rejectionCode) {
    private static QuotaDecision acceptedDecision() {
      return new QuotaDecision(true, null);
    }

    private static QuotaDecision rejectedDecision(String rejectionCode) {
      return new QuotaDecision(false, rejectionCode);
    }
  }
}
