package dev.concord.resolver.merchant;

import java.util.List;
import java.util.Optional;

/**
 * The merchant's view of its orders, through a small adapter: the sample merchant implements it over
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

    /** The full order, or empty if the merchant does not know it (or cannot be reached). */
    Optional<MerchantOrder> getOrder(String orderId);

    default OrderStatus orderStatus(String orderId) {
        return getOrder(orderId).map(o -> o.status().equals("CANCELLED") ? OrderStatus.CANCELLED : OrderStatus.VALID)
                .orElse(OrderStatus.UNKNOWN);
    }

    /**
     * Orders changed after a cursor, oldest first (keyset pagination). Lets the divergence detector notice
     * merchant-side changes (a timeout cancelling an order) without polling every order.
     */
    default Page changedSince(Cursor cursor, int limit) {
        return new Page(List.of(), cursor);
    }

    record Cursor(String since, String after) {
        public static Cursor start() {
            return new Cursor("1970-01-01T00:00:00Z", "");
        }
    }

    record Page(List<MerchantOrder> orders, Cursor next) {
    }
}
