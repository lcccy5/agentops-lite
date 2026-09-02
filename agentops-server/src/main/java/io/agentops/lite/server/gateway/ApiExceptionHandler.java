package io.agentops.lite.server.gateway;

import io.agentops.lite.contract.Contracts.ApiError;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Maps expected failures to a stable JSON envelope. */
@RestControllerAdvice
@Order(-2)
public final class ApiExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);
    /** Maps domain failures without leaking internal stack traces. */
    @ExceptionHandler(GatewayException.class)
    public ResponseEntity<ApiError> handleGateway(GatewayException exception) {
        return ResponseEntity.status(exception.status()).body(error(exception.code(), exception.getMessage()));
    }

    /** Maps malformed input to a client error. */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleInput(IllegalArgumentException exception) {
        return ResponseEntity.badRequest().body(error("INVALID_ARGUMENT", exception.getMessage()));
    }

    /** Prevents unexpected internals from being exposed. */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception exception) {
        LOG.error("Unexpected API failure", exception);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error("INTERNAL_ERROR", "Unexpected server error"));
    }

    private ApiError error(String code, String message) { return new ApiError(code, message, UUID.randomUUID().toString(), Instant.now()); }
}
