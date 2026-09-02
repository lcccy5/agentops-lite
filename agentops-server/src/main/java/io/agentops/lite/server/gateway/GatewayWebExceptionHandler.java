package io.agentops.lite.server.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.ApiError;
import java.time.Instant;
import java.util.UUID;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

/** Serializes authentication failures raised before controller advice can participate. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class GatewayWebExceptionHandler implements WebExceptionHandler {
    private final ObjectMapper mapper;

    /** Creates the filter-chain error writer. */
    public GatewayWebExceptionHandler(ObjectMapper mapper) { this.mapper = mapper; }

    /** Writes expected gateway errors and delegates every unrelated exception. */
    @Override public Mono<Void> handle(ServerWebExchange exchange, Throwable exception) {
        if (!(exception instanceof GatewayException gateway)) return Mono.error(exception);
        try {
            byte[] body = mapper.writeValueAsBytes(new ApiError(gateway.code(), gateway.getMessage(), UUID.randomUUID().toString(), Instant.now()));
            exchange.getResponse().setStatusCode(gateway.status()); exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
            return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
        } catch (Exception serializationFailure) { return Mono.error(serializationFailure); }
    }
}
