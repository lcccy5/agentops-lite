package io.agentops.lite.server.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.agentops.lite.core.domain.UsageModels.ConfirmedUsage;
import org.junit.jupiter.api.Test;

class ProviderUsageAccumulatorTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void usesTerminalUsageAndGenerationIdAcrossArbitraryTransportChunks() {
    ProviderUsageAccumulator accumulator = new ProviderUsageAccumulator(mapper);
    accumulator.accept(
        "data: {\"id\":\"generation-1\",\"choices\":[{\"delta\":{\"content\":\"你\"}}]}\r"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    accumulator.accept(
        "\n\r\ndata: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":4}}\n\n"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));

    ConfirmedUsage usage = accumulator.usage(request());

    assertThat(accumulator.generationId()).isEqualTo("generation-1");
    assertThat(usage.estimated()).isFalse();
    assertThat(usage.inputTokens()).isEqualTo(7);
    assertThat(usage.outputTokens()).isEqualTo(4);
  }

  @Test
  void ignoresIncompleteUsageInsteadOfTreatingItAsAuthoritativeZero() {
    ProviderUsageAccumulator accumulator = new ProviderUsageAccumulator(mapper);
    accumulator.accept(
        "data: {\"usage\":{\"prompt_tokens\":3}}\n\n"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));

    ConfirmedUsage usage = accumulator.usage(request());

    assertThat(usage.estimated()).isTrue();
    assertThat(usage.outputTokens()).isPositive();
  }

  private ObjectNode request() {
    ObjectNode request = mapper.createObjectNode();
    request.putArray("messages").addObject().put("role", "user").put("content", "测试");
    return request;
  }
}
