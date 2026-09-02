package io.agentops.lite.core.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.AgentEvalObservation;
import io.agentops.lite.contract.Contracts.EvalCaseDefinition;
import io.agentops.lite.contract.Contracts.ToolObservation;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Covers positive scoring and explicit safety/evidence failures. */
class DeterministicEvalScorerTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final DeterministicEvalScorer scorer = new DeterministicEvalScorer();

    @Test void passesMatchingObservableBehavior() {
        var expected = new EvalCaseDefinition("case", "question", "fixture", List.of("metrics"), Map.of("period", mapper.getNodeFactory().textNode("ONE_YEAR")), List.of("METRIC"), List.of("最近一年"), List.of("保证收益"), false);
        var actual = new AgentEvalObservation("run", "最近一年风险较高", List.of(new ToolObservation("metrics", mapper.createObjectNode().put("period", "ONE_YEAR"))), List.of("METRIC"), 10, 5, 2, 20);
        var score = scorer.score(expected, actual);
        assertThat(score.passed()).isTrue();
        assertThat(score.toolSelectionPassed()).isTrue();
        assertThat(score.argumentsPassed()).isTrue();
        assertThat(score.evidencePassed()).isTrue();
        assertThat(score.answerPassed()).isTrue();
    }

    @Test void explainsEveryFailedConstraint() {
        var expected = new EvalCaseDefinition("case", "question", "fixture", List.of("metrics"), Map.of(), List.of("METRIC"), List.of("风险"), List.of("保证收益"), true);
        var actual = new AgentEvalObservation("run", "保证收益", List.of(), List.of(), 10, 5, 2, 20);
        var score = scorer.score(expected, actual);
        assertThat(score.failures()).containsExactly("missing tool: metrics", "missing evidence: METRIC", "answer missing: 风险", "forbidden claim: 保证收益");
        assertThat(score.toolSelectionPassed()).isFalse();
        assertThat(score.argumentsPassed()).isTrue();
        assertThat(score.evidencePassed()).isFalse();
        assertThat(score.answerPassed()).isFalse();
    }
}
