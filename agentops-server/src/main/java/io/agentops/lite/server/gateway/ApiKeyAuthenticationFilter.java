package io.agentops.lite.server.gateway;

import io.agentops.lite.server.config.AgentOpsProperties;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/** Authenticates online traffic by hash and management traffic by local admin token. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class ApiKeyAuthenticationFilter implements WebFilter {
    public static final String PROJECT_ATTRIBUTE = "agentops.projectId";
    private final JdbcTemplate jdbc;
    private final AgentOpsProperties properties;
    private final Scheduler blockingScheduler;

    /** Creates the filter with database-backed API key lookup. */
    public ApiKeyAuthenticationFilter(JdbcTemplate jdbc, AgentOpsProperties properties, Scheduler blockingScheduler) {
        this.jdbc = jdbc; this.properties = properties; this.blockingScheduler = blockingScheduler;
    }

    /** Authenticates scoped endpoints and leaves health endpoints public. */
    @Override public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (path.startsWith("/actuator/")) return chain.filter(exchange);
        if (path.startsWith("/console")) {
            if (!validConsoleCredentials(exchange)) return rejectConsoleLogin(exchange);
            exchange.getAttributes().put(PROJECT_ATTRIBUTE, "project-fund-agent");
            return chain.filter(exchange);
        }
        if (path.startsWith("/internal/")) {
            String token = exchange.getRequest().getHeaders().getFirst("X-AgentOps-Admin-Token");
            if (!MessageDigest.isEqual(bytes(properties.adminToken()), bytes(token))) {
                return Mono.error(new GatewayException("ADMIN_UNAUTHORIZED", HttpStatus.UNAUTHORIZED, "Invalid admin token"));
            }
            exchange.getAttributes().put(PROJECT_ATTRIBUTE, "project-fund-agent");
            return chain.filter(exchange);
        }
        String authorization = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        String token = authorization != null && authorization.startsWith("Bearer ") ? authorization.substring(7) : null;
        if (token == null || token.isBlank()) return Mono.error(new GatewayException("API_KEY_REQUIRED", HttpStatus.UNAUTHORIZED, "Bearer API key is required"));
        return Mono.fromCallable(() -> projectFor(token)).subscribeOn(blockingScheduler).flatMap(projectId -> {
            String claimed = exchange.getRequest().getHeaders().getFirst("X-AgentOps-Project-Id");
            if (claimed != null && !claimed.equals(projectId)) return Mono.error(new GatewayException("PROJECT_MISMATCH", HttpStatus.FORBIDDEN, "Project header conflicts with API key"));
            exchange.getAttributes().put(PROJECT_ATTRIBUTE, projectId);
            return chain.filter(exchange);
        });
    }

    /** Validates browser Basic authentication without placing the admin token in a URL or page script. */
    private boolean validConsoleCredentials(ServerWebExchange exchange) {
        String authorization = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authorization == null || !authorization.startsWith("Basic ")) return false;
        try {
            String decoded = new String(Base64.getDecoder().decode(authorization.substring(6)), StandardCharsets.UTF_8);
            int separator = decoded.indexOf(':');
            if (separator < 0 || !"admin".equals(decoded.substring(0, separator))) return false;
            return MessageDigest.isEqual(bytes(properties.adminToken()), bytes(decoded.substring(separator + 1)));
        } catch (IllegalArgumentException malformedBase64) {
            return false;
        }
    }

    /** Triggers the browser-native login dialog while keeping the console dependency-free. */
    private Mono<Void> rejectConsoleLogin(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().set(HttpHeaders.WWW_AUTHENTICATE, "Basic realm=\"AgentOps Console\"");
        return exchange.getResponse().setComplete();
    }

    private String projectFor(String token) {
        var rows = jdbc.query("select project_id from project_api_key where key_hash=? and enabled=true", (rs, row) -> rs.getString(1), sha256(token));
        if (rows.isEmpty()) throw new GatewayException("API_KEY_INVALID", HttpStatus.UNAUTHORIZED, "Invalid API key");
        return rows.getFirst();
    }

    private static byte[] bytes(String value) { return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8); }
    private static String sha256(String value) {
        try { return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes(value))); }
        catch (NoSuchAlgorithmException exception) { throw new IllegalStateException(exception); }
    }
}
