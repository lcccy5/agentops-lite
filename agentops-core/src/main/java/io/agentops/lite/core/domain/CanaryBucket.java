package io.agentops.lite.core.domain;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Cross-language stable SHA-256 canary allocation. */
public final class CanaryBucket {
    private CanaryBucket() { }

    /** Returns a deterministic bucket in the inclusive range 0..99. */
    public static int calculate(String subjectKey, String releaseId) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest((subjectKey + ":" + releaseId).getBytes(StandardCharsets.UTF_8));
            long value = ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
            return (int) Long.remainderUnsigned(value, 100);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 must be available", exception);
        }
    }
}
