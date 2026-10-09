package dev.concord.resolver.merchant;

/**
 * The merchant's view of an order, through a small adapter: the sample merchant implements it over
 * HTTP + PostgreSQL; a real merchant implements it against their own system. Concord never reads the
 * merchant's database directly.
 */
public interface MerchantOrders {

    enum OrderStatus {
        /** The order still wants this payment: capture it and save the sale. */
        VALID,
        /** The order is gone (timed out, cancelled): release the money now instead of days later. */
        CANCELLED,
        /** Cannot tell; do nothing. */
        UNKNOWN
    }

    OrderStatus orderStatus(String orderId);
}
