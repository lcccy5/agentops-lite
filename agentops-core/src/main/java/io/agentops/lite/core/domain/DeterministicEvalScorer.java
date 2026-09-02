package io.agentops.lite.core.domain;

import com.fasterxml.jackson.databind.JsonNode;
import io.agentops.lite.contract.Contracts.AgentEvalObservation;
import io.agentops.lite.contract.Contracts.EvalCaseDefinition;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Scores observable Agent behavior without an unstable LLM judge. */
public final class DeterministicEvalScorer {
    /** Scores tool selection, arguments, evidence and answer constraints with explicit failures. */
    public Score score(EvalCaseDefinition expected, AgentEvalObservation actual) {
        List<String> toolFailures = new ArrayList<>();
        List<String> argumentFailures = new ArrayList<>();
        List<String> evidenceFailures = new ArrayList<>();
        List<String> answerFailures = new ArrayList<>();
        Set<String> tools = new HashSet<>();
        safe(actual.tools()).forEach(tool -> tools.add(tool.name()));
        for (String tool : safe(expected.expectedTools())) if (!tools.contains(tool)) toolFailures.add("missing tool: " + tool);
        for (Map.Entry<String, JsonNode> argument : safeMap(expected.expectedArguments()).entrySet()) {
            boolean matched = safe(actual.tools()).stream().anyMatch(tool -> tool.arguments() != null && tool.arguments().findValue(argument.getKey()) != null && tool.arguments().findValue(argument.getKey()).equals(argument.getValue()));
            if (!matched) argumentFailures.add("missing argument: " + argument.getKey() + "=" + argument.getValue());
        }
        for (String evidence : safe(expected.requiredEvidenceTypes())) if (!safe(actual.evidenceTypes()).contains(evidence)) evidenceFailures.add("missing evidence: " + evidence);
        String answer = actual.answer() == null ? "" : actual.answer();
        for (String pattern : safe(expected.requiredAnswerPatterns())) if (!answer.contains(pattern)) answerFailures.add("answer missing: " + pattern);
        for (String forbidden : safe(expected.forbiddenClaims())) if (answer.contains(forbidden)) answerFailures.add("forbidden claim: " + forbidden);
        List<String> failures = new ArrayList<>();
        failures.addAll(toolFailures);
        failures.addAll(argumentFailures);
        failures.addAll(evidenceFailures);
        failures.addAll(answerFailures);
        return new Score(failures.isEmpty(), toolFailures.isEmpty(), argumentFailures.isEmpty(),
                evidenceFailures.isEmpty(), answerFailures.isEmpty(), List.copyOf(failures));
    }

    private static <T> List<T> safe(List<T> value) { return value == null ? List.of() : value; }
    private static <K, V> Map<K, V> safeMap(Map<K, V> value) { return value == null ? Map.of() : value; }

    /** Pass/fail outcome with separately gateable quality dimensions and auditable reasons. */
    public record Score(boolean passed, boolean toolSelectionPassed, boolean argumentsPassed,
                        boolean evidencePassed, boolean answerPassed, List<String> failures) { }
}
