package io.agentops.lite.server.usage;

import io.agentops.lite.server.gateway.ApiKeyAuthenticationFilter;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

/** Read-only management endpoints for auditable usage facts and reconciliation findings. */
@RestController
@RequestMapping("/internal/v1/usage")
public final class UsageQueryController {
    private final UsageService usage;
    private final JdbcTemplate jdbc;

    /** Creates the query facade. */
    public UsageQueryController(UsageService usage, JdbcTemplate jdbc) { this.usage = usage; this.jdbc = jdbc; }

    /** Queries one request by its externally visible identifier. */
    @GetMapping("/queryRequest/{requestId}")
    public Map<String, Object> queryRequest(@PathVariable String requestId, ServerWebExchange exchange) { return usage.queryRequest(project(exchange), requestId); }

    /** Queries all model calls correlated to one upstream Agent run. */
    @GetMapping("/queryRun/{correlationId}")
    public Map<String, Object> queryRun(@PathVariable String correlationId, ServerWebExchange exchange) {
        return usage.queryRun(exchange.getAttribute(ApiKeyAuthenticationFilter.PROJECT_ATTRIBUTE), correlationId);
    }

    /** Lists recent correlated Agent runs for operational inspection. */
    @GetMapping("/queryRecentRuns")
    public List<Map<String, Object>> queryRecentRuns(ServerWebExchange exchange) {
        return usage.queryRecentRuns(exchange.getAttribute(ApiKeyAuthenticationFilter.PROJECT_ATTRIBUTE), 100);
    }

    /** Compares immutable-ledger and Kafka-projection totals. */
    @GetMapping("/querySummary")
    public Map<String, Object> querySummary(ServerWebExchange exchange) { return usage.querySummary(exchange.getAttribute(ApiKeyAuthenticationFilter.PROJECT_ATTRIBUTE)); }

    /** Lists recent discrepancies without mutating accounting facts. */
    @GetMapping("/queryReconciliations")
    public List<Map<String, Object>> queryReconciliations(ServerWebExchange exchange) {
        return jdbc.queryForList("select reconciliation_id,project_id,discrepancy_type,expected_value,actual_value,suggested_action,detected_at from usage_reconciliation where project_id=? order by detected_at desc limit 100", project(exchange));
    }

    /** Appends a correction instead of mutating the original estimated ledger. */
    @PostMapping("/adjustEstimatedRequest/{requestId}")
    public Map<String, Object> adjustEstimatedRequest(@PathVariable String requestId, @RequestBody UsageAdjustmentRequest request, ServerWebExchange exchange) {
        return usage.adjustEstimatedUsage(project(exchange), requestId, request.correctedTokens());
    }

    /** Reads the authenticated project chosen by API-key or privileged management context. */
    private String project(ServerWebExchange exchange) { return exchange.getAttribute(ApiKeyAuthenticationFilter.PROJECT_ATTRIBUTE); }

    /** Explicit correction payload using the final confirmed token total. */
    public record UsageAdjustmentRequest(long correctedTokens) { }
}
