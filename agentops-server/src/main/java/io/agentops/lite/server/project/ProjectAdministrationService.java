package io.agentops.lite.server.project;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import io.agentops.lite.server.gateway.GatewayException;

/** Manages an Agent product's isolated quota, credentials and operator-facing health view. */
@Service
public final class ProjectAdministrationService {
    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final TransactionTemplate transactions;

    /** Creates project administration over persistent facts and the online Redis quota state. */
    public ProjectAdministrationService(JdbcTemplate jdbc, StringRedisTemplate redis, TransactionTemplate transactions) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.transactions = transactions;
    }

    /** Lists Agent products with their durable usage total for the platform switcher. */
    public List<Map<String, Object>> queryProjects() {
        return jdbc.queryForList("""
                select p.project_id,p.name,p.token_limit,p.max_concurrency,p.default_max_tokens,p.project_max_tokens,p.created_at,
                       coalesce(u.total_tokens,0) ledger_tokens
                from agent_project p left join usage_projection u on u.project_id=p.project_id
                order by p.created_at
                """);
    }

    /** Creates one isolated Agent product and its first operational budget. */
    public Map<String, Object> createProject(CreateProjectRequest request) {
        validateQuota(request.tokenLimit(), request.maxConcurrency(), request.defaultMaxTokens(), request.projectMaxTokens());
        try {
            jdbc.update("insert into agent_project(project_id,name,token_limit,max_concurrency,default_max_tokens,project_max_tokens) values(?,?,?,?,?,?)",
                    request.projectId(), request.name(), request.tokenLimit(), request.maxConcurrency(), request.defaultMaxTokens(), request.projectMaxTokens());
        } catch (DuplicateKeyException duplicate) {
            throw new GatewayException("PROJECT_ALREADY_EXISTS", HttpStatus.CONFLICT, "Project ID already exists");
        }
        audit(request.projectId(), "PROJECT_CREATED", Map.of(), request.asMap());
        return queryProjectOverview(request.projectId());
    }

    /** Returns durable budget settings, online holds and consumption for one isolated Agent product. */
    public Map<String, Object> queryProjectOverview(String projectId) {
        Map<String, Object> project = jdbc.queryForMap("select project_id,name,token_limit,max_concurrency,default_max_tokens,project_max_tokens,created_at from agent_project where project_id=?", projectId);
        long consumed = redisNumber(projectId, "consumed");
        long reserved = redisNumber(projectId, "reserved");
        long active = redisNumber(projectId, "active");
        long tokenLimit = ((Number) project.get("token_limit")).longValue();
        Map<String, Object> view = new LinkedHashMap<>(project);
        view.put("consumedTokens", consumed);
        view.put("reservedTokens", reserved);
        view.put("availableTokens", Math.max(0, tokenLimit - consumed - reserved));
        view.put("activeRequests", active);
        view.put("ledgerTokens", jdbc.queryForObject("select coalesce(sum(token_delta),0) from usage_ledger where project_id=?", Long.class, projectId));
        view.put("apiKeyCount", jdbc.queryForObject("select count(*) from project_api_key where project_id=? and enabled=true", Integer.class, projectId));
        return view;
    }

    /** Updates admission limits for future requests and writes an immutable operator audit record. */
    public Map<String, Object> updateProjectQuota(String projectId, UpdateQuotaRequest request) {
        validateQuota(request.tokenLimit(), request.maxConcurrency(), request.defaultMaxTokens(), request.projectMaxTokens());
        Map<String, Object> before = jdbc.queryForMap("select token_limit,max_concurrency,default_max_tokens,project_max_tokens from agent_project where project_id=?", projectId);
        int changed = jdbc.update("update agent_project set token_limit=?,max_concurrency=?,default_max_tokens=?,project_max_tokens=? where project_id=?",
                request.tokenLimit(), request.maxConcurrency(), request.defaultMaxTokens(), request.projectMaxTokens(), projectId);
        if (changed != 1) throw new GatewayException("PROJECT_NOT_FOUND", HttpStatus.NOT_FOUND, "Project does not exist");
        audit(projectId, "QUOTA_UPDATED", before, request.asMap());
        return queryProjectOverview(projectId);
    }

    /** Creates a one-time plaintext API key; only its SHA-256 digest is retained after this response. */
    public Map<String, Object> createProjectApiKey(String projectId, CreateApiKeyRequest request) {
        ensureProject(projectId);
        String plaintext = "agop_" + UUID.randomUUID().toString().replace("-", "");
        String keyId = UUID.randomUUID().toString();
        jdbc.update("insert into project_api_key(api_key_id,project_id,key_hash,enabled,created_at) values(?,?,?,true,?)",
                keyId, projectId, sha256(plaintext), Instant.now());
        audit(projectId, "API_KEY_CREATED", Map.of(), Map.of("apiKeyId", keyId, "label", request.label()));
        return Map.of("apiKeyId", keyId, "label", request.label(), "apiKey", plaintext,
                "message", "Copy this key now. It cannot be shown again.");
    }

    /** Lists key metadata without exposing any secret material. */
    public List<Map<String, Object>> queryProjectApiKeys(String projectId) {
        ensureProject(projectId);
        return jdbc.queryForList("select api_key_id,enabled,created_at from project_api_key where project_id=? order by created_at desc", projectId);
    }

    /** Enables or revokes a project credential without deleting the audit trail. */
    public Map<String, Object> updateProjectApiKeyStatus(String projectId, String apiKeyId, UpdateApiKeyStatusRequest request) {
        int changed = jdbc.update("update project_api_key set enabled=? where api_key_id=? and project_id=?", request.enabled(), apiKeyId, projectId);
        if (changed != 1) throw new GatewayException("API_KEY_NOT_FOUND", HttpStatus.NOT_FOUND, "API key does not belong to this project");
        audit(projectId, request.enabled() ? "API_KEY_ENABLED" : "API_KEY_REVOKED", Map.of("apiKeyId", apiKeyId), Map.of("enabled", request.enabled()));
        return Map.of("apiKeyId", apiKeyId, "enabled", request.enabled());
    }

    private void ensureProject(String projectId) {
        Integer count = jdbc.queryForObject("select count(*) from agent_project where project_id=?", Integer.class, projectId);
        if (count == null || count != 1) throw new GatewayException("PROJECT_NOT_FOUND", HttpStatus.NOT_FOUND, "Project does not exist");
    }
    private long redisNumber(String projectId, String field) {
        Object value = redis.opsForHash().get("agentops:quota:" + projectId, field);
        return value == null ? 0 : Long.parseLong(value.toString());
    }
    private void audit(String projectId, String action, Map<String, Object> before, Map<String, Object> after) {
        transactions.executeWithoutResult(status -> jdbc.update("insert into project_configuration_audit(audit_id,project_id,action,before_json,after_json,created_at) values(?,?,?,?,?,?)",
                UUID.randomUUID().toString(), projectId, action, before.toString(), after.toString(), Instant.now()));
    }
    private static void validateQuota(long tokenLimit, int maxConcurrency, int defaultMaxTokens, int projectMaxTokens) {
        if (tokenLimit < 1 || maxConcurrency < 1 || defaultMaxTokens < 1 || projectMaxTokens < defaultMaxTokens) {
            throw new IllegalArgumentException("Token limit, concurrency and output limits must be positive; project maximum must cover the default");
        }
    }
    private static String sha256(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException exception) { throw new IllegalStateException(exception); }
    }

    /** Request for creating an Agent product, whose project ID is its durable isolation boundary. */
    public record CreateProjectRequest(String projectId, String name, long tokenLimit, int maxConcurrency, int defaultMaxTokens, int projectMaxTokens) {
        Map<String, Object> asMap() { return Map.of("name", name, "tokenLimit", tokenLimit, "maxConcurrency", maxConcurrency, "defaultMaxTokens", defaultMaxTokens, "projectMaxTokens", projectMaxTokens); }
    }
    /** Request for updating limits that apply to future admissions for one Agent product. */
    public record UpdateQuotaRequest(long tokenLimit, int maxConcurrency, int defaultMaxTokens, int projectMaxTokens) {
        Map<String, Object> asMap() { return Map.of("tokenLimit", tokenLimit, "maxConcurrency", maxConcurrency, "defaultMaxTokens", defaultMaxTokens, "projectMaxTokens", projectMaxTokens); }
    }
    /** Request metadata for an API key; the label is retained only in the audit record in this version. */
    public record CreateApiKeyRequest(String label) { }
    /** Request for enabling or revoking an API key. */
    public record UpdateApiKeyStatusRequest(boolean enabled) { }
}
