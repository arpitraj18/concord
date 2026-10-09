package dev.concord.delivery;

import dev.concord.engine.store.OutboxEntry;

/**
 * What the merchant receives: one change of one payment, final and verified as far as Concord knows.
 *
 * @param idempotencyKey gateway#paymentId#state#version: store it, and ignore any event whose key you
 *                       have already processed (delivery is at-least-once)
 * @param version        increases with every change of the payment: ignore an event older than the
 *                       last one you applied
 * @param kind           TRANSITION, or CORRECTION when an earlier event was wrong and this replaces it
 */
public record MerchantEvent(
        String idempotencyKey,
        String kind,
        String gateway,
        String paymentId,
        String orderId,
        String state,
        String previousState,
        long version,
        Long amountMinor,
        String currency,
        String occurredAt) {

    public static MerchantEvent of(OutboxEntry e) {
        return new MerchantEvent(e.idempotencyKey(), e.kind().name(), e.gateway().name(), e.paymentId(), e.orderId(),
                e.toState().name(), e.fromState().name(), e.version(), e.amountMinor(), e.currency(),
                e.createdAt().toString());
    }
}
