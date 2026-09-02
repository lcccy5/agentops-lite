package io.agentops.lite.server.controlplane;

import io.agentops.lite.contract.Contracts.CreateEvalJobRequest;
import io.agentops.lite.contract.Contracts.CreatePromptVersionRequest;
import io.agentops.lite.contract.Contracts.CreateReleaseRequest;
import io.agentops.lite.contract.Contracts.ImportDatasetRequest;
import io.agentops.lite.contract.Contracts.ResolvePromptRequest;
import io.agentops.lite.contract.Contracts.ResolvedPrompt;
import io.agentops.lite.server.gateway.ApiKeyAuthenticationFilter;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

/** Action-oriented local management API for versioning, evaluation, release and rollback. */
@RestController
@RequestMapping("/internal/v1")
public final class ControlPlaneController {
    private final ControlPlaneService service;

    /** Creates the management API. */
    public ControlPlaneController(ControlPlaneService service) { this.service = service; }

    /** Creates an immutable prompt version. */
    @PostMapping("/prompts/createVersion/{promptKey}")
    public Map<String, Object> createPromptVersion(@PathVariable String promptKey, @Valid @RequestBody CreatePromptVersionRequest request, ServerWebExchange exchange) {
        return service.createPromptVersion(project(exchange), promptKey, request);
    }

    /** Resolves a stable or candidate prompt for one subject. */
    @PostMapping("/prompts/resolvePrompt/{promptKey}")
    public ResolvedPrompt resolvePrompt(@PathVariable String promptKey, @Valid @RequestBody ResolvePromptRequest request, ServerWebExchange exchange) {
        return service.resolve(project(exchange), promptKey, request.environment(), request.subjectKey(), request.forcedVersion());
    }

    /** Imports an atomic deterministic dataset. */
    @PostMapping("/evaluations/importDataset")
    public Map<String, Object> importDataset(@Valid @RequestBody ImportDatasetRequest request, ServerWebExchange exchange) {
        return service.importDataset(project(exchange), request);
    }

    /** Creates two evaluation tasks per case through an outbox. */
    @PostMapping("/evaluations/createJob")
    public Map<String, Object> createEvaluationJob(@Valid @RequestBody CreateEvalJobRequest request, ServerWebExchange exchange) {
        return service.createEvalJob(project(exchange), request);
    }

    /** Queries aggregate job progress. */
    @GetMapping("/evaluations/queryJob/{jobId}")
    public Map<String, Object> queryEvaluationJob(@PathVariable String jobId) { return service.job(jobId); }

    /** Queries deterministic case-level observations. */
    @GetMapping("/evaluations/queryResults/{jobId}")
    public List<Map<String, Object>> queryEvaluationResults(@PathVariable String jobId) { return service.results(jobId); }

    /** Creates a release only after its referenced gate passed. */
    @PostMapping("/releases/createRelease")
    public Map<String, Object> createRelease(@Valid @RequestBody CreateReleaseRequest request, ServerWebExchange exchange) {
        return service.createRelease(project(exchange), request);
    }

    /** Immediately changes an active release to rolled back. */
    @PostMapping("/releases/rollbackRelease/{releaseId}")
    public Map<String, Object> rollbackRelease(@PathVariable String releaseId) { return service.rollback(releaseId); }

    /** Queries one release and its current state. */
    @GetMapping("/releases/queryRelease/{releaseId}")
    public Map<String, Object> queryRelease(@PathVariable String releaseId) { return service.release(releaseId); }

    private String project(ServerWebExchange exchange) { return exchange.getAttribute(ApiKeyAuthenticationFilter.PROJECT_ATTRIBUTE); }
}
