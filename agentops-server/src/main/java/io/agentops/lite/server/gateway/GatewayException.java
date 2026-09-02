package io.agentops.lite.server.gateway;

import org.springframework.http.HttpStatus;

/** Expected gateway failure with a stable code and HTTP status. */
public final class GatewayException extends RuntimeException {
    private final String code;
    private final HttpStatus status;

    /** Creates a safe client-facing failure. */
    public GatewayException(String code, HttpStatus status, String message) {
        super(message); this.code = code; this.status = status;
    }

    /** Returns the stable failure code. */
    public String code() { return code; }
    /** Returns the mapped HTTP status. */
    public HttpStatus status() { return status; }
}
