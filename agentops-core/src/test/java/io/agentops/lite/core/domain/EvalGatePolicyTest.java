package io.agentops.lite.core.domain;

import org.junit.jupiter.api.Test;
import io.agentops.lite.core.domain.EvalGatePolicy.QualityMetrics;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that release quality and cost gates cannot be bypassed. */
class EvalGatePolicyTest {
    private final EvalGatePolicy policy = new EvalGatePolicy();

    @Test void passesNonRegressingCandidateWithinCostBudget() {
        assertThat(policy.decide(0, metrics(.8, .8, .7, .9), metrics(.9, .9, .8, .9), 100, 109, 10).passed()).isTrue();
    }

    @Test void rejectsSafetyAggregateAndCostRegressions() {
        assertThat(policy.decide(1, metrics(.9, .8, .8, .8), metrics(.8, .8, .8, .8), 100, 121, 10).reasons()).hasSize(3);
    }

    @Test void rejectsEachHiddenQualityDimensionRegression() {
        var decision = policy.decide(0, metrics(.5, .9, .8, .7), metrics(.6, .8, .7, .6), 100, 100, 10);
        assertThat(decision.reasons()).containsExactly(
                "candidate tool-selection pass rate regressed from 0.9 to 0.8",
                "candidate argument pass rate regressed from 0.8 to 0.7",
                "candidate evidence pass rate regressed from 0.7 to 0.6");
    }

    private static QualityMetrics metrics(double pass, double tool, double arguments, double evidence) {
        return new QualityMetrics(pass, tool, arguments, evidence);
    }
}
