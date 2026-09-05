package io.agentops.lite.server.project;

import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Platform-admin API for managing isolated Agent products, their budgets and credentials. */
@RestController
@RequestMapping("/internal/v1/projects")
public final class ProjectAdministrationController {
    private final ProjectAdministrationService projects;

    /** Creates the project administration API. */
    public ProjectAdministrationController(ProjectAdministrationService projects) { this.projects = projects; }

    /** Queries every Agent product available to the platform administrator. */
    @GetMapping("/queryProjects")
    public List<Map<String, Object>> queryProjects() { return projects.queryProjects(); }

    /** Creates a new Agent product with its own durable quota namespace. */
    @PostMapping("/createProject")
    public Map<String, Object> createProject(@RequestBody ProjectAdministrationService.CreateProjectRequest request) { return projects.createProject(request); }

    /** Queries budget, online quota and credential state for one Agent product. */
    @GetMapping("/queryProjectOverview/{projectId}")
    public Map<String, Object> queryProjectOverview(@PathVariable String projectId) { return projects.queryProjectOverview(projectId); }

    /** Updates future admission limits for one isolated Agent product. */
    @PostMapping("/updateProjectQuota/{projectId}")
    public Map<String, Object> updateProjectQuota(@PathVariable String projectId, @RequestBody ProjectAdministrationService.UpdateQuotaRequest request) { return projects.updateProjectQuota(projectId, request); }

    /** Creates a new API key and returns its plaintext value exactly once. */
    @PostMapping("/createProjectApiKey/{projectId}")
    public Map<String, Object> createProjectApiKey(@PathVariable String projectId, @RequestBody ProjectAdministrationService.CreateApiKeyRequest request) { return projects.createProjectApiKey(projectId, request); }

    /** Lists non-secret API key metadata for one Agent product. */
    @GetMapping("/queryProjectApiKeys/{projectId}")
    public List<Map<String, Object>> queryProjectApiKeys(@PathVariable String projectId) { return projects.queryProjectApiKeys(projectId); }

    /** Enables or revokes one API key without deleting its audit trail. */
    @PostMapping("/updateProjectApiKeyStatus/{projectId}/{apiKeyId}")
    public Map<String, Object> updateProjectApiKeyStatus(@PathVariable String projectId, @PathVariable String apiKeyId,
                                                           @RequestBody ProjectAdministrationService.UpdateApiKeyStatusRequest request) {
        return projects.updateProjectApiKeyStatus(projectId, apiKeyId, request);
    }
}
