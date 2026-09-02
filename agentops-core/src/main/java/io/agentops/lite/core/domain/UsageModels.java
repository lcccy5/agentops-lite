package io.agentops.lite.core.domain;

import java.math.BigDecimal;
import java.time.Instant;

/** Usage state machine and immutable value models. */
public final class UsageModels {
    private UsageModels() { }

    /** Database-visible reservation lifecycle. */
    public enum ReservationStatus { PENDING, RESERVED, SETTLED, CANCELLED, FAILED, REJECTED, RECONCILIATION_PENDING }

    /** Immutable ledger entry kinds; corrections are appended as adjustments. */
    public enum LedgerType { USAGE_ACTUAL, USAGE_ESTIMATED, USAGE_ADJUSTMENT, RESERVATION_RELEASE }

    /** Admission result required before a provider can be called. */
    public record Reservation(String reservationId, String requestId, String projectId,
                              String idempotencyKey, long reservedTokens, ReservationStatus status,
                              Instant expiresAt) { }

    /** Usage confirmed during normal completion or inferred after interruption. */
    public record ConfirmedUsage(long inputTokens, long outputTokens, boolean estimated) {
        /** Returns the non-negative total token charge. */
        public long totalTokens() { return Math.max(0, inputTokens) + Math.max(0, outputTokens); }
    }

    /** Immutable accounting delta. */
    public record LedgerEntry(String ledgerId, String reservationId, String projectId,
                              LedgerType type, String relatedLedgerId, long tokenDelta,
                              BigDecimal costDelta, String promptVersion, Instant occurredAt) { }
}
