package io.agentops.lite.server.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.core.domain.UsageModels.ConfirmedUsage;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Parses an SSE stream across arbitrary network chunks and retains only settlement evidence. Raw
 * diagnostic capture may be capped, but this accumulator must receive every chunk so a terminal
 * usage event after the diagnostic cap is still authoritative.
 */
final class ProviderUsageAccumulator {
  /** Jackson parser shared with the gateway protocol adapter. */
  private final ObjectMapper mapper;

  /** Incomplete raw SSE bytes retained until the next network chunk arrives. */
  private final ByteArrayOutputStream pending = new ByteArrayOutputStream();

  /** Visible semantic output used only by the documented fallback estimator. */
  private final StringBuilder output = new StringBuilder();

  /** Last non-null usage object; protocols that emit cumulative values update this field. */
  private JsonNode latestUsage;

  /** Provider request identifier observed in any valid event. */
  private String generationId;

  /** Creates a stateful parser for one upstream response stream. */
  ProviderUsageAccumulator(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /**
   * Accepts one arbitrary byte chunk. Parsing waits for a blank-line event boundary so JSON split
   * by TCP framing cannot be treated as a malformed or missing usage event.
   */
  void accept(byte[] bytes) {
    pending.writeBytes(bytes);
    byte[] available = pending.toByteArray();
    int start = 0;
    int boundary;
    while ((boundary = eventBoundary(available, start)) >= 0) {
      String event = new String(available, start, boundary - start, StandardCharsets.UTF_8);
      parseEvent(event);
      start = boundary + boundaryWidth(available, boundary);
    }
    if (start > 0) {
      pending.reset();
      pending.write(available, start, available.length - start);
    }
  }

  /**
   * Returns the provider's final usage when present, otherwise a versioned visible-content
   * estimate.
   */
  ConfirmedUsage usage(JsonNode request) {
    if (latestUsage != null
        && latestUsage.hasNonNull("prompt_tokens")
        && latestUsage.hasNonNull("completion_tokens")) {
      return new ConfirmedUsage(
          latestUsage.path("prompt_tokens").asLong(),
          latestUsage.path("completion_tokens").asLong(),
          false);
    }
    long input = Math.max(1, request.path("messages").toString().length() / 3L);
    long estimatedOutput = Math.max(1, output.length() / 4L);
    return new ConfirmedUsage(input, estimatedOutput, true);
  }

  /** Validates provider usage before treating it as an accounting fact. */
  private static boolean validUsage(JsonNode usage) {
    return usage != null
        && usage.path("prompt_tokens").canConvertToLong()
        && usage.path("completion_tokens").canConvertToLong()
        && usage.path("prompt_tokens").longValue() >= 0
        && usage.path("completion_tokens").longValue() >= 0;
  }

  /** Returns the generation ID only after it has been parsed from a complete event. */
  String generationId() {
    return generationId;
  }

  /** Locates either CRLF or LF SSE event separators without assuming one transport convention. */
  private static int eventBoundary(byte[] value, int start) {
    for (int index = start; index + 1 < value.length; index++) {
      if (value[index] != '\n') continue;
      if (value[index + 1] == '\n') return index;
      if (index + 2 < value.length && value[index + 1] == '\r' && value[index + 2] == '\n')
        return index - 1;
    }
    return -1;
  }

  /** Returns the number of separator characters at the chosen boundary. */
  private static int boundaryWidth(byte[] value, int boundary) {
    return boundary + 3 < value.length
            && value[boundary] == '\r'
            && value[boundary + 1] == '\n'
            && value[boundary + 2] == '\r'
            && value[boundary + 3] == '\n'
        ? 4
        : 2;
  }

  /** Extracts only known JSON fields and ignores heartbeat, DONE, and unknown future events. */
  private void parseEvent(String event) {
    for (String line : event.split("\\R")) {
      if (!line.startsWith("data:")) continue;
      String payload = line.substring(5).trim();
      if (payload.equals("[DONE]")) return;
      try {
        JsonNode node = mapper.readTree(payload);
        String id = node.path("id").asText("");
        if (!id.isBlank()) generationId = id;
        JsonNode usage = node.path("usage");
        if (!usage.isMissingNode() && !usage.isNull()) latestUsage = usage;
        for (JsonNode choice : node.path("choices")) {
          String content = choice.path("delta").path("content").asText("");
          if (!content.isEmpty()) output.append(content);
        }
      } catch (Exception ignored) {
        /* Invalid provider events remain diagnostic-only. */
      }
    }
  }
}
