package io.agentops.lite.contract;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Shared HTTP and Kafka contracts kept independent from both executable processes. */
public final class Contracts {
    private Contracts() { }

    /** Stable machine-readable API failure. */
    public record ApiError(String code, String message, String requestId, Instant timestamp) { }

    /** Immutable usage delta published from the transactional outbox. */
    public record UsageLedgerEvent(String ledgerId, String projectId, String reservationId,
                                   String ledgerType, long tokenDelta, BigDecimal costDelta,
                                   String promptVersion, Instant occurredAt) { }

    /** A single independently distributable evaluation case. */
    public record EvalCaseEvent(String jobId, String caseId, String promptVersion) { }

    /** Creates an immutable prompt version. */
    public record CreatePromptVersionRequest(@NotBlank String version, @NotBlank String template) { }

    /** Resolves a prompt for one stable canary subject. */
    public record ResolvePromptRequest(@NotBlank String environment, @NotBlank String subjectKey,
                                       String forcedVersion) { }

    /** Resolved prompt and release metadata injected into an Agent request. */
    public record ResolvedPrompt(String promptVersion, String releaseId, String variant,
                                 String template, String templateHash) { }

    /** Creates a gated release from stable and candidate prompt versions. */
    public record CreateReleaseRequest(@NotBlank String promptKey, @NotBlank String environment,
                                       @NotBlank String stableVersion, @NotBlank String candidateVersion,
                                       @NotNull Integer canaryPercent, @NotBlank String gateResultId) { }

    /** Imports one named collection of deterministic evaluation cases. */
    public record ImportDatasetRequest(@NotBlank String name, @NotNull List<EvalCaseDefinition> cases) { }

    /** Defines observable expectations without using an LLM judge. */
    public record EvalCaseDefinition(@NotBlank String caseId, @NotBlank String question,
                                     @NotBlank String fixtureId, List<String> expectedTools,
                                     Map<String, JsonNode> expectedArguments,
                                     List<String> requiredEvidenceTypes,
                                     List<String> requiredAnswerPatterns,
                                     List<String> forbiddenClaims, boolean hardSafety) { }

    /** Creates a stable-or-candidate evaluation run. */
    public record CreateEvalJobRequest(@NotBlank String datasetId, @NotBlank String promptKey,
                                       @NotBlank String stableVersion, @NotBlank String candidateVersion,
                                       long maxAverageTokenGrowthPercent) { }

    /** Request accepted by the fund Agent's local-only evaluation target. */
    public record AgentEvalRequest(String caseId, String promptVersion, String question, String fixtureId) { }

    /** Structured observations used by deterministic scoring. */
    public record AgentEvalObservation(String runId, String answer, List<ToolObservation> tools,
                                       List<String> evidenceTypes, long inputTokens, long outputTokens,
                                       long firstTokenMillis, long totalMillis) { }

    /** Captures the actual tool name and arguments emitted by orchestration. */
    public record ToolObservation(String name, JsonNode arguments) { }
}
