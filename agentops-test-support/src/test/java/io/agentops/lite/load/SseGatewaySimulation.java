package io.agentops.lite.load;

import static io.gatling.javaapi.core.CoreDsl.asLongAs;
import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static io.gatling.javaapi.core.CoreDsl.StringBody;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.sse;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import java.time.Duration;

/** Small, honest local SSE load scenario covering completion and active cancellation. */
public final class SseGatewaySimulation extends Simulation {
    private static final String REQUEST = "{\"model\":\"deterministic-fund-model\",\"stream\":true,\"stream_options\":{\"include_usage\":true},\"max_tokens\":128,\"messages\":[{\"role\":\"user\",\"content\":\"分析基金风险\"}]}";
    private final HttpProtocolBuilder protocol = http.baseUrl(System.getProperty("baseUrl", "http://localhost:18080"))
            .acceptHeader("text/event-stream").authorizationHeader("Bearer agentops-dev-key")
            .sseUnmatchedInboundMessageBufferSize(64);

    private final ScenarioBuilder complete = scenario("SSE completes")
            .exec(sse("open-complete").post("/v1/chat/completions").body(StringBody(REQUEST)).asJson())
            .exec(session -> session.set("stop", false))
            .asLongAs(session -> !session.getBoolean("stop")).on(
                    sse.processUnmatchedMessages((messages, session) -> messages.stream().anyMatch(message -> message.message().contains("[DONE]")) ? session.set("stop", true) : session))
            .exec(sse("close-complete").close());

    private final ScenarioBuilder cancel = scenario("SSE client cancels")
            .exec(sse("open-cancel").post("/v1/chat/completions").body(StringBody(REQUEST)).asJson())
            .pause(Duration.ofMillis(100)).exec(sse("cancel-stream").close());

    /** Configures ten completion users and ten cancelling users without claiming production scale. */
    public SseGatewaySimulation() {
        setUp(complete.injectOpen(atOnceUsers(10)), cancel.injectOpen(atOnceUsers(10))).protocols(protocol)
                .assertions(global().failedRequests().percent().lte(0.0));
    }
}
