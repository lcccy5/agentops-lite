package io.agentops.lite.server.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.core.domain.UsageModels.ConfirmedUsage;
import io.agentops.lite.core.domain.UsageModels.Reservation;
import io.agentops.lite.server.config.AgentOpsProperties;
import io.agentops.lite.server.usage.UsageService;
import io.micrometer.core.instrument.MeterRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

/** OpenAI-compatible online entry with admission, transparent SSE and cancellation-safe accounting. */
@RestController
public final class ChatCompletionsGateway {
    private static final int MAX_CAPTURE_BYTES = 2 * 1024 * 1024;
    private final WebClient provider;
    private final UsageService usage;
    private final AgentOpsProperties properties;
    private final ObjectMapper mapper;
    private final ExecutorService finalizer;
    private final Scheduler blockingScheduler;
    private final MeterRegistry meters;
    private final AtomicInteger activeConnections = new AtomicInteger();
    private final CircuitBreaker providerCircuit;

    /** Creates the online gateway. */
    public ChatCompletionsGateway(WebClient providerWebClient, UsageService usage, AgentOpsProperties properties,
                                  ObjectMapper mapper, ExecutorService blockingExecutor,
                                  Scheduler blockingScheduler, MeterRegistry meters, CircuitBreakerRegistry circuitBreakers) {
        this.provider = providerWebClient; this.usage = usage; this.properties = properties; this.mapper = mapper;
        this.finalizer = blockingExecutor; this.blockingScheduler = blockingScheduler; this.meters = meters;
        meters.gauge("agentops.gateway.active_connections", activeConnections);
        this.providerCircuit = circuitBreakers.circuitBreaker("provider");
    }

    /** Proxies the exact compatibility route required by Spring AI clients. */
    @PostMapping(path = "/v1/chat/completions", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<Void> createChatCompletion(@RequestBody JsonNode request, ServerWebExchange exchange) {
        String projectId = exchange.getAttribute(ApiKeyAuthenticationFilter.PROJECT_ATTRIBUTE);
        String requestId = first(exchange.getRequest().getHeaders().getFirst("X-AgentOps-Request-Id"), UUID.randomUUID().toString());
        String idempotencyKey = first(exchange.getRequest().getHeaders().getFirst("Idempotency-Key"), requestId);
        String promptVersion = exchange.getRequest().getHeaders().getFirst("Prompt-Version");
        String releaseId = exchange.getRequest().getHeaders().getFirst("Release-Id");
        String variant = exchange.getRequest().getHeaders().getFirst("Variant");
        return Mono.fromCallable(() -> usage.reserve(projectId, requestId, idempotencyKey, request))
                .subscribeOn(blockingScheduler)
                .flatMap(reservation -> request.path("stream").asBoolean(false)
                        ? stream(request, exchange.getResponse(), reservation, promptVersion, releaseId, variant)
                        : ordinary(request, exchange.getResponse(), reservation, promptVersion, releaseId, variant));
    }

    private Mono<Void> ordinary(JsonNode request, ServerHttpResponse response, Reservation reservation,
                                String promptVersion, String releaseId, String variant) {
        long started = System.nanoTime();
        if (!providerCircuit.tryAcquirePermission()) {
            finishAsync(reservation, zeroUsage(), "FAILED", promptVersion);
            return Mono.error(new GatewayException("PROVIDER_CIRCUIT_OPEN", HttpStatus.SERVICE_UNAVAILABLE, "Provider circuit is open"));
        }
        usage.markProviderStarted(reservation.reservationId());
        return provider.post().uri("/v1/chat/completions").headers(headers -> upstreamHeaders(headers, reservation, promptVersion, releaseId, variant))
                .bodyValue(request).exchangeToMono(upstream -> readOrdinary(upstream, response))
                .flatMap(bytes -> {
                    response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
                    return response.writeWith(Mono.just(response.bufferFactory().wrap(bytes)))
                            .doOnSuccess(ignored -> finishAsync(reservation, usageFromJson(bytes, request, false), "SETTLED", promptVersion));
                })
                .doOnSuccess(ignored -> providerCircuit.onSuccess(System.nanoTime() - started, java.util.concurrent.TimeUnit.NANOSECONDS))
                .doOnError(error -> {
                    providerCircuit.onError(System.nanoTime() - started, java.util.concurrent.TimeUnit.NANOSECONDS, error);
                    finishAsync(reservation, failureUsage(request, error), "FAILED", promptVersion);
                })
                .doFinally(signal -> meters.timer("agentops.gateway.duration", "mode", "ordinary", "signal", signal.name())
                        .record(Duration.ofNanos(System.nanoTime() - started)));
    }

    private Mono<byte[]> readOrdinary(ClientResponse upstream, ServerHttpResponse downstream) {
        downstream.setStatusCode(upstream.statusCode());
        if (upstream.statusCode().isError()) return upstream.bodyToMono(byte[].class)
                .flatMap(bytes -> Mono.error(new GatewayException("PROVIDER_" + upstream.statusCode().value(), mapStatus(upstream.statusCode().value()), safeProviderMessage(bytes))));
        return upstream.bodyToMono(byte[].class);
    }

    private Mono<Void> stream(JsonNode request, ServerHttpResponse response, Reservation reservation,
                              String promptVersion, String releaseId, String variant) {
        ByteArrayOutputStream capture = new ByteArrayOutputStream(); AtomicBoolean finalized = new AtomicBoolean();
        AtomicBoolean firstToken = new AtomicBoolean(); AtomicLong started = new AtomicLong(System.nanoTime());
        AtomicReference<Throwable> streamFailure = new AtomicReference<>();
        if (!providerCircuit.tryAcquirePermission()) {
            finishAsync(reservation, zeroUsage(), "FAILED", promptVersion);
            return Mono.error(new GatewayException("PROVIDER_CIRCUIT_OPEN", HttpStatus.SERVICE_UNAVAILABLE, "Provider circuit is open"));
        }
        response.setStatusCode(HttpStatus.OK); response.getHeaders().setContentType(MediaType.TEXT_EVENT_STREAM);
        Flux<DataBuffer> body = provider.post().uri("/v1/chat/completions")
                .headers(headers -> upstreamHeaders(headers, reservation, promptVersion, releaseId, variant)).bodyValue(request)
                .exchangeToFlux(upstream -> readStream(upstream))
                .doOnNext(bytes -> {
                    if (firstToken.compareAndSet(false, true)) meters.timer("agentops.gateway.first_token").record(Duration.ofNanos(System.nanoTime() - started.get()));
                    if (capture.size() + bytes.length <= MAX_CAPTURE_BYTES) capture.writeBytes(bytes);
                })
                .onBackpressureBuffer(Math.max(8, properties.streamBufferSize()), ignored -> meters.counter("agentops.gateway.buffer_overflow").increment(), reactor.core.publisher.BufferOverflowStrategy.ERROR)
                .map(bytes -> response.bufferFactory().wrap(bytes))
                .doOnComplete(() -> providerCircuit.onSuccess(System.nanoTime() - started.get(), java.util.concurrent.TimeUnit.NANOSECONDS))
                .doOnError(error -> {
                    streamFailure.set(error);
                    providerCircuit.onError(System.nanoTime() - started.get(), java.util.concurrent.TimeUnit.NANOSECONDS, error);
                })
                .doOnSubscribe(ignored -> { usage.markProviderStarted(reservation.reservationId()); activeConnections.incrementAndGet(); meters.counter("agentops.gateway.connections", "event", "opened").increment(); })
                .doFinally(signal -> {
                    if (signal == SignalType.CANCEL) providerCircuit.onError(System.nanoTime() - started.get(), java.util.concurrent.TimeUnit.NANOSECONDS, new java.util.concurrent.CancellationException("downstream cancelled"));
                    activeConnections.updateAndGet(value -> Math.max(0, value - 1)); meters.counter("agentops.gateway.connections", "event", "closed", "signal", signal.name()).increment();
                    if (finalized.compareAndSet(false, true)) {
                        boolean interrupted = signal == SignalType.CANCEL || signal == SignalType.ON_ERROR;
                        ConfirmedUsage confirmed = capture.size() == 0 && isProviderRejection(streamFailure.get())
                                ? zeroUsage() : usageFromSse(capture.toByteArray(), request, interrupted);
                        finishAsync(reservation, confirmed, interrupted ? "CANCELLED" : "SETTLED", promptVersion);
                    }
                });
        return response.writeWith(body);
    }

    private Flux<byte[]> readStream(ClientResponse upstream) {
        if (upstream.statusCode().isError()) return upstream.bodyToMono(byte[].class)
                .flatMapMany(bytes -> Flux.error(new GatewayException("PROVIDER_" + upstream.statusCode().value(), mapStatus(upstream.statusCode().value()), safeProviderMessage(bytes))));
        return upstream.bodyToFlux(byte[].class);
    }

    private void upstreamHeaders(HttpHeaders headers, Reservation reservation, String promptVersion, String releaseId, String variant) {
        headers.setBearerAuth(properties.providerApiKey()); headers.set("X-AgentOps-Request-Id", reservation.requestId());
        put(headers, "Prompt-Version", promptVersion); put(headers, "Release-Id", releaseId); put(headers, "Variant", variant);
    }

    private void finishAsync(Reservation reservation, ConfirmedUsage confirmed, String state, String promptVersion) {
        finalizer.submit(() -> usage.finalizeReservation(reservation, confirmed, state, promptVersion));
    }

    private ConfirmedUsage usageFromJson(byte[] bytes, JsonNode request, boolean partial) {
        try {
            JsonNode root = mapper.readTree(bytes); JsonNode usageNode = root.path("usage");
            if (!usageNode.isMissingNode()) return new ConfirmedUsage(usageNode.path("prompt_tokens").asLong(), usageNode.path("completion_tokens").asLong(), false);
            String completion = root.path("choices").path(0).path("message").path("content").asText("");
            ConfirmedUsage fallback = fallbackUsage(request, partial);
            return new ConfirmedUsage(fallback.inputTokens(), Math.max(0, completion.length() / 4L), true);
        } catch (Exception ignored) { }
        return fallbackUsage(request, partial);
    }

    private ConfirmedUsage usageFromSse(byte[] bytes, JsonNode request, boolean partial) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        for (String line : text.split("\\R")) {
            if (!line.startsWith("data:")) continue;
            String json = line.substring(5).trim();
            if (json.equals("[DONE]")) continue;
            try {
                JsonNode node = mapper.readTree(json).path("usage");
                if (!node.isMissingNode() && !node.isNull()) return new ConfirmedUsage(node.path("prompt_tokens").asLong(), node.path("completion_tokens").asLong(), false);
            } catch (Exception ignored) { }
        }
        long outputEstimate = Math.max(1, text.length() / 4L);
        ConfirmedUsage base = fallbackUsage(request, partial);
        return new ConfirmedUsage(base.inputTokens(), Math.min(base.outputTokens(), outputEstimate), true);
    }

    private ConfirmedUsage fallbackUsage(JsonNode request, boolean partial) {
        long input = Math.max(1, request.path("messages").toString().length() / 3L);
        long output = partial ? 1 : Math.max(1, request.path("max_tokens").asLong(properties.defaultMaxTokens()) / 2);
        return new ConfirmedUsage(input, output, true);
    }

    /** Releases the reservation fully when the provider rejected work before producing a response. */
    private ConfirmedUsage failureUsage(JsonNode request, Throwable error) {
        return isProviderRejection(error) ? zeroUsage() : fallbackUsage(request, false);
    }

    private static boolean isProviderRejection(Throwable error) {
        return error instanceof GatewayException gateway && gateway.code().startsWith("PROVIDER_");
    }

    private static ConfirmedUsage zeroUsage() { return new ConfirmedUsage(0, 0, false); }

    private String safeProviderMessage(byte[] bytes) { String value = new String(bytes, StandardCharsets.UTF_8); return value.length() > 300 ? value.substring(0, 300) : value; }
    private HttpStatus mapStatus(int status) { return status == 429 ? HttpStatus.TOO_MANY_REQUESTS : status >= 500 ? HttpStatus.BAD_GATEWAY : HttpStatus.valueOf(status); }
    private static String first(String value, String fallback) { return value == null || value.isBlank() ? fallback : value; }
    private static void put(HttpHeaders headers, String name, String value) { if (value != null && !value.isBlank()) headers.set(name, value); }
}
