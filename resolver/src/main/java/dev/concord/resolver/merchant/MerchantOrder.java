package dev.concord.resolver.merchant;

import java.time.Instant;

/**
 * The merchant's own view of an order, as its adapter reports it.
 *
 * @param status       the merchant's order status (sample merchant: AWAITING_PAYMENT, PAID, PAYMENT_FAILED,
 *                     REFUNDED, CANCELLED)
 * @param paymentState the last payment state the merchant applied from Concord, or null
 */
public record MerchantOrder(String orderId, String gateway, String paymentId, String status, Long amountMinor,
                            String currency, String paymentState, Instant updatedAt) {
}
