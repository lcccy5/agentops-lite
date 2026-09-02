package io.agentops.lite.core.domain;

import java.util.ArrayList;
import java.util.List;

/** Compares stable and candidate aggregate metrics for release eligibility. */
public final class EvalGatePolicy {
    /** Rejects safety failures, aggregate or dimension-level regressions, and excessive token growth. */
    public Decision decide(long candidateHardSafetyFailures, QualityMetrics stable, QualityMetrics candidate,
                           double stableAverageTokens, double candidateAverageTokens, long maxTokenGrowthPercent) {
        List<String> reasons = new ArrayList<>();
        if (candidateHardSafetyFailures > 0) reasons.add("candidate failed hard-safety cases");
        rejectRegression(reasons, "overall pass rate", stable.passRate(), candidate.passRate());
        rejectRegression(reasons, "tool-selection pass rate", stable.toolSelectionRate(), candidate.toolSelectionRate());
        rejectRegression(reasons, "argument pass rate", stable.argumentRate(), candidate.argumentRate());
        rejectRegression(reasons, "evidence pass rate", stable.evidenceRate(), candidate.evidenceRate());
        if (stableAverageTokens > 0 && candidateAverageTokens > stableAverageTokens * (1D + maxTokenGrowthPercent / 100D)) reasons.add("candidate token usage exceeded threshold");
        return new Decision(reasons.isEmpty(), List.copyOf(reasons));
    }

    private static void rejectRegression(List<String> reasons, String dimension, double stable, double candidate) {
        if (candidate < stable) reasons.add("candidate " + dimension + " regressed from " + stable + " to " + candidate);
    }

    /** Stable/candidate quality rates that must independently remain non-regressing. */
    public record QualityMetrics(double passRate, double toolSelectionRate, double argumentRate,
                                 double evidenceRate) { }

    /** Release decision with reasons persisted by the worker. */
    public record Decision(boolean passed, List<String> reasons) { }
}
