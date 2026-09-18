package io.agentops.lite.load;

import static io.gatling.javaapi.core.CoreDsl.StringBody;
import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.constantConcurrentUsers;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.rampConcurrentUsers;
import static io.gatling.javaapi.core.CoreDsl.rampUsers;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.sse;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import java.time.Duration;
import java.util.UUID;

/** Small, honest local SSE load scenario covering completion and active cancellation. */
public final class SseGatewaySimulation extends Simulation {
  private static final int COMPLETE_USERS = Integer.getInteger("completeUsers", 10);
  private static final int CANCEL_USERS = Integer.getInteger("cancelUsers", 10);
  private static final int RAMP_SECONDS = Integer.getInteger("rampSeconds", 0);
  private static final int STEADY_SECONDS = Integer.getInteger("steadySeconds", 0);
  private static final int CANCEL_AFTER_MS = Integer.getInteger("cancelAfterMs", 150);
  private static final int MAX_P95_MS = Integer.getInteger("maxP95Ms", 2000);
  private static final String COMPLETE_REQUEST =
      request(STEADY_SECONDS > 0 ? "soak-complete" : "load-complete");
  private static final String CANCEL_REQUEST =
      request(STEADY_SECONDS > 0 ? "soak-slow" : "load-slow");
  private final HttpProtocolBuilder protocol =
      http.baseUrl(System.getProperty("baseUrl", "http://localhost:18080"))
          .acceptHeader("text/event-stream")
          .authorizationHeader("Bearer agentops-dev-key")
          .sseUnmatchedInboundMessageBufferSize(64);

  private final ScenarioBuilder complete =
      scenario("SSE completes")
          .exec(session -> session.set("requestId", UUID.randomUUID().toString()))
          .exec(
              sse("open-complete")
                  .post("/v1/chat/completions")
                  .header("X-AgentOps-Request-Id", "#{requestId}")
                  .header("Idempotency-Key", "#{requestId}")
                  .body(StringBody(COMPLETE_REQUEST))
                  .asJson())
          .exec(session -> session.set("stop", false))
          .asLongAs(session -> !session.isFailed() && !session.getBoolean("stop"))
          .on(
              sse.processUnmatchedMessages(
                  (messages, session) ->
                      messages.stream().anyMatch(message -> message.message().contains("[DONE]"))
                          ? session.set("stop", true)
                          : session))
          .exec(sse("close-complete").close());

  private final ScenarioBuilder cancel =
      scenario("SSE client cancels")
          .exec(session -> session.set("requestId", UUID.randomUUID().toString()))
          .exec(
              sse("open-cancel")
                  .post("/v1/chat/completions")
                  .header("X-AgentOps-Request-Id", "#{requestId}")
                  .header("Idempotency-Key", "#{requestId}")
                  .body(StringBody(CANCEL_REQUEST))
                  .asJson())
          .pause(Duration.ofMillis(CANCEL_AFTER_MS))
          .exec(sse("cancel-stream").close());

  /** Configures ten completion users and ten cancelling users without claiming production scale. */
  public SseGatewaySimulation() {
    if (STEADY_SECONDS > 0) {
      setUp(
              complete.injectClosed(
                  rampConcurrentUsers(0)
                      .to(COMPLETE_USERS)
                      .during(Duration.ofSeconds(Math.max(1, RAMP_SECONDS))),
                  constantConcurrentUsers(COMPLETE_USERS)
                      .during(Duration.ofSeconds(STEADY_SECONDS))),
              cancel.injectClosed(
                  rampConcurrentUsers(0)
                      .to(CANCEL_USERS)
                      .during(Duration.ofSeconds(Math.max(1, RAMP_SECONDS))),
                  constantConcurrentUsers(CANCEL_USERS).during(Duration.ofSeconds(STEADY_SECONDS))))
          .protocols(protocol)
          .assertions(
              global().failedRequests().percent().lte(0.0),
              global().responseTime().percentile(95.0).lte(MAX_P95_MS));
      return;
    }
    var completeInjection =
        RAMP_SECONDS > 0
            ? complete.injectOpen(
                rampUsers(COMPLETE_USERS).during(Duration.ofSeconds(RAMP_SECONDS)))
            : complete.injectOpen(atOnceUsers(COMPLETE_USERS));
    var cancelInjection =
        RAMP_SECONDS > 0
            ? cancel.injectOpen(rampUsers(CANCEL_USERS).during(Duration.ofSeconds(RAMP_SECONDS)))
            : cancel.injectOpen(atOnceUsers(CANCEL_USERS));
    setUp(completeInjection, cancelInjection)
        .protocols(protocol)
        .assertions(
            global().failedRequests().percent().lte(0.0),
            global().responseTime().percentile(95.0).lte(MAX_P95_MS));
  }

  private static String request(String model) {
    return "{\"model\":\""
        + model
        + "\",\"stream\":true,\"stream_options\":{\"include_usage\":true},"
        + "\"max_tokens\":128,\"messages\":[{\"role\":\"user\",\"content\":\"分析基金风险\"}]}";
  }
}
