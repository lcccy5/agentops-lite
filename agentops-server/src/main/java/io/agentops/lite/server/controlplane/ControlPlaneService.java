package io.agentops.lite.server.controlplane;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts;
import io.agentops.lite.contract.Contracts.CreateEvalJobRequest;
import io.agentops.lite.contract.Contracts.CreatePromptVersionRequest;
import io.agentops.lite.contract.Contracts.CreateReleaseRequest;
import io.agentops.lite.contract.Contracts.EvalCaseEvent;
import io.agentops.lite.contract.Contracts.ImportDatasetRequest;
import io.agentops.lite.contract.Contracts.ResolvedPrompt;
import io.agentops.lite.core.domain.CanaryBucket;
import io.agentops.lite.server.gateway.GatewayException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Transactional Prompt, evaluation dispatch, release and rollback control plane. */
@Service
public final class ControlPlaneService {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper mapper;

    /** Creates the control-plane service. */
    public ControlPlaneService(JdbcTemplate jdbc, TransactionTemplate transactions, ObjectMapper mapper) {
        this.jdbc = jdbc; this.transactions = transactions; this.mapper = mapper;
    }

    /** Creates an immutable version and returns its generated identifier. */
    public Map<String, Object> createPromptVersion(String projectId, String promptKey, CreatePromptVersionRequest request) {
        String id = UUID.randomUUID().toString(); String hash = sha256(request.template()); Instant now = Instant.now();
        jdbc.update("insert into prompt_version(prompt_version_id,project_id,prompt_key,version,template_text,template_hash,created_at) values(?,?,?,?,?,?,?)",
                id, projectId, promptKey, request.version(), request.template(), hash, now);
        return Map.of("promptVersionId", id, "promptKey", promptKey, "version", request.version(), "templateHash", hash);
    }

    /** Resolves forced evaluation versions or the latest active stable canary release. */
    public ResolvedPrompt resolve(String projectId, String promptKey, String environment, String subjectKey, String forcedVersion) {
        if (forcedVersion != null && !forcedVersion.isBlank()) return prompt(projectId, promptKey, forcedVersion, null, "forced");
        List<Map<String, Object>> releases = jdbc.queryForList("select release_id,stable_version,candidate_version,canary_percent from prompt_release where project_id=? and prompt_key=? and environment_name=? and status='ACTIVE' order by created_at desc limit 1",
                projectId, promptKey, environment);
        if (releases.isEmpty()) {
            // A rollback is a durable routing decision; never fall through to a newer candidate prompt.
            List<Map<String, Object>> rolledBack = jdbc.queryForList("select release_id,stable_version from prompt_release where project_id=? and prompt_key=? and environment_name=? and status='ROLLED_BACK' order by rolled_back_at desc limit 1",
                    projectId, promptKey, environment);
            if (!rolledBack.isEmpty()) {
                Map<String, Object> release = rolledBack.getFirst();
                return prompt(projectId, promptKey, release.get("stable_version").toString(), release.get("release_id").toString(), "rollback-stable");
            }
            List<String> versions = jdbc.query("select version from prompt_version where project_id=? and prompt_key=? order by created_at desc limit 1", (rs, row) -> rs.getString(1), projectId, promptKey);
            if (versions.isEmpty()) throw new GatewayException("PROMPT_NOT_FOUND", HttpStatus.NOT_FOUND, "No prompt version exists");
            return prompt(projectId, promptKey, versions.getFirst(), null, "stable");
        }
        Map<String, Object> release = releases.getFirst(); String releaseId = release.get("release_id").toString();
        int percent = ((Number) release.get("canary_percent")).intValue(); boolean candidate = CanaryBucket.calculate(subjectKey, releaseId) < percent;
        String version = release.get(candidate ? "candidate_version" : "stable_version").toString();
        return prompt(projectId, promptKey, version, releaseId, candidate ? "candidate" : "stable");
    }

    /** Imports all cases atomically so jobs never observe a partial dataset. */
    public Map<String, Object> importDataset(String projectId, ImportDatasetRequest request) {
        String datasetId = UUID.randomUUID().toString(); Instant now = Instant.now();
        transactions.executeWithoutResult(status -> {
            jdbc.update("insert into eval_dataset(dataset_id,project_id,name,created_at) values(?,?,?,?)", datasetId, projectId, request.name(), now);
            for (Contracts.EvalCaseDefinition definition : request.cases()) {
                jdbc.update("insert into eval_case(case_id,dataset_id,definition_json) values(?,?,?)", definition.caseId(), datasetId, json(definition));
            }
        });
        return Map.of("datasetId", datasetId, "caseCount", request.cases().size());
    }

    /** Creates stable and candidate tasks plus reliable dispatch outbox rows in one transaction. */
    public Map<String, Object> createEvalJob(String projectId, CreateEvalJobRequest request) {
        String jobId = UUID.randomUUID().toString(); Instant now = Instant.now();
        Integer promptCount = jdbc.queryForObject("select count(*) from prompt_version where project_id=? and prompt_key=? and version in (?,?)",
                Integer.class, projectId, request.promptKey(), request.stableVersion(), request.candidateVersion());
        if (promptCount == null || promptCount != 2) throw new GatewayException("PROMPT_VERSION_NOT_FOUND", HttpStatus.BAD_REQUEST, "Stable and candidate versions must both belong to this project and prompt key");
        List<String> cases = jdbc.query("select c.case_id from eval_case c join eval_dataset d on d.dataset_id=c.dataset_id where c.dataset_id=? and d.project_id=? order by c.case_id",
                (rs, row) -> rs.getString(1), request.datasetId(), projectId);
        if (cases.isEmpty()) throw new GatewayException("DATASET_EMPTY", HttpStatus.BAD_REQUEST, "Dataset has no cases");
        transactions.executeWithoutResult(status -> {
            jdbc.update("insert into eval_job(job_id,project_id,dataset_id,prompt_key,stable_version,candidate_version,max_token_growth_percent,status,created_at) values(?,?,?,?,?,?,?,'PENDING',?)",
                    jobId, projectId, request.datasetId(), request.promptKey(), request.stableVersion(), request.candidateVersion(), request.maxAverageTokenGrowthPercent(), now);
            for (String caseId : cases) for (String version : List.of(request.stableVersion(), request.candidateVersion())) {
                jdbc.update("insert into eval_job_case(job_id,case_id,prompt_version,status) values(?,?,?,'PENDING')", jobId, caseId, version);
                EvalCaseEvent event = new EvalCaseEvent(jobId, caseId, version);
                jdbc.update("insert into eval_dispatch_outbox(event_id,job_id,case_id,prompt_version,event_key,payload_json,status,next_attempt_at,created_at) values(?,?,?,?,?,?,'PENDING',?,?)",
                        UUID.randomUUID().toString(), jobId, caseId, version, caseId, json(event), now, now);
            }
        });
        return Map.of("jobId", jobId, "taskCount", cases.size() * 2, "status", "PENDING");
    }

    /** Creates only a 0, 5 or 100 percent release backed by a passing gate. */
    public Map<String, Object> createRelease(String projectId, CreateReleaseRequest request) {
        if (!List.of(0, 5, 100).contains(request.canaryPercent())) throw new GatewayException("INVALID_CANARY_PERCENT", HttpStatus.BAD_REQUEST, "canaryPercent must be 0, 5 or 100");
        Integer matchingGate = jdbc.queryForObject("select count(*) from eval_gate_result g join eval_job j on j.job_id=g.job_id where g.gate_result_id=? and g.passed=true and j.project_id=? and j.prompt_key=? and j.stable_version=? and j.candidate_version=?",
                Integer.class, request.gateResultId(), projectId, request.promptKey(), request.stableVersion(), request.candidateVersion());
        if (matchingGate == null || matchingGate != 1) throw new GatewayException("EVAL_GATE_REJECTED", HttpStatus.CONFLICT, "Passing gate must match this project, prompt key and exact versions");
        String id = UUID.randomUUID().toString(); Instant now = Instant.now();
        transactions.executeWithoutResult(status -> {
            jdbc.update("update prompt_release set status='SUPERSEDED' where project_id=? and prompt_key=? and environment_name=? and status='ACTIVE'", projectId, request.promptKey(), request.environment());
            jdbc.update("insert into prompt_release(release_id,project_id,prompt_key,environment_name,stable_version,candidate_version,canary_percent,gate_result_id,status,created_at) values(?,?,?,?,?,?,?,?, 'ACTIVE',?)",
                    id, projectId, request.promptKey(), request.environment(), request.stableVersion(), request.candidateVersion(), request.canaryPercent(), request.gateResultId(), now);
        });
        return release(id);
    }

    /** Marks the selected release rolled back; uncached resolutions immediately select stable. */
    public Map<String, Object> rollback(String releaseId) {
        int changed = jdbc.update("update prompt_release set status='ROLLED_BACK',canary_percent=0,rolled_back_at=? where release_id=? and status='ACTIVE'", Instant.now(), releaseId);
        if (changed != 1) throw new GatewayException("RELEASE_NOT_ACTIVE", HttpStatus.CONFLICT, "Release is not active");
        return release(releaseId);
    }

    /** Returns a release diagnostic view. */
    public Map<String, Object> release(String releaseId) {
        return jdbc.queryForMap("select release_id,prompt_key,environment_name,stable_version,candidate_version,canary_percent,gate_result_id,status,created_at,rolled_back_at from prompt_release where release_id=?", releaseId);
    }

    /** Returns job status and its derived completion counts. */
    public Map<String, Object> job(String jobId) {
        Map<String, Object> row = jdbc.queryForMap("select job_id,dataset_id,prompt_key,stable_version,candidate_version,status,created_at,completed_at from eval_job where job_id=?", jobId);
        row.put("tasks", jdbc.queryForList("select status,count(*) count from eval_job_case where job_id=? group by status", jobId));
        List<Map<String, Object>> gate = jdbc.queryForList("select gate_result_id,passed,reasons_json,created_at from eval_gate_result where job_id=?", jobId);
        row.put("gate", gate.isEmpty() ? null : gate.getFirst());
        return row;
    }

    /** Returns all persisted observations and deterministic scores for a job. */
    public List<Map<String, Object>> results(String jobId) {
        return jdbc.queryForList("select result_id,case_id,prompt_version,passed,hard_safety,score_json,observation_json,input_tokens,output_tokens,first_token_millis,total_millis,created_at from eval_result where job_id=? order by case_id,prompt_version", jobId);
    }

    private ResolvedPrompt prompt(String projectId, String promptKey, String version, String releaseId, String variant) {
        return jdbc.queryForObject("select version,template_text,template_hash from prompt_version where project_id=? and prompt_key=? and version=?",
                (rs, row) -> new ResolvedPrompt(rs.getString(1), releaseId, variant, rs.getString(2), rs.getString(3)), projectId, promptKey, version);
    }
    private String json(Object value) { try { return mapper.writeValueAsString(value); } catch (JsonProcessingException exception) { throw new IllegalStateException(exception); } }
    private static String sha256(String value) { try { return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); } catch (NoSuchAlgorithmException exception) { throw new IllegalStateException(exception); } }
}
