package io.agentops.lite.core.domain;

import com.fasterxml.jackson.databind.JsonNode;

/** Conservative provider-independent estimator used only for admission and fallback settlement. */
public final class TokenEstimator {
    private TokenEstimator() { }

    /** Estimates UTF-8 mixed-language input and reserves the requested output plus safety margin. */
    public static long reserve(JsonNode request, int defaultMaxTokens, int projectMaxTokens, int safetyMargin) {
        int requested = request.path("max_tokens").asInt(defaultMaxTokens);
        if (requested <= 0 || requested > projectMaxTokens) {
            throw new IllegalArgumentException("max_tokens must be between 1 and " + projectMaxTokens);
        }
        long input = Math.max(1, (request.path("messages").toString().length() + 2L) / 3L);
        long tools = Math.max(0, request.path("tools").toString().length() / 4L);
        return input + tools + requested + safetyMargin;
    }
}
