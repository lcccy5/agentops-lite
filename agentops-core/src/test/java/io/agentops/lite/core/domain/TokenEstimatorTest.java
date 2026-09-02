package io.agentops.lite.core.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies conservative admission estimates and project output limits. */
class TokenEstimatorTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test void includesInputOutputToolAndSafetyBudgets() throws Exception {
        var request = mapper.readTree("{\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}],\"tools\":[{\"type\":\"function\"}],\"max_tokens\":100}");
        assertThat(TokenEstimator.reserve(request, 200, 500, 32)).isGreaterThan(132);
    }

    @Test void rejectsOutputAboveProjectLimit() throws Exception {
        var request = mapper.readTree("{\"messages\":[],\"max_tokens\":501}");
        assertThatThrownBy(() -> TokenEstimator.reserve(request, 200, 500, 32)).isInstanceOf(IllegalArgumentException.class);
    }
}
