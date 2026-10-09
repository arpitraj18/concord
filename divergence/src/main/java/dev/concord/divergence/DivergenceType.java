package dev.concord.divergence;

/**
 * Ways an order can disagree with what really happened to its payment. Neither the gateway's
 * dashboard nor the merchant's database shows any of these on its own: each looks fine alone.
 *
 * @param weight how much money or trust is at stake, relative to the others (severity multiplier)
 */
public enum DivergenceType {
    /** The customer was charged, but the order is cancelled or failed: refunds, chargebacks, lost trust. */
    PAID_BUT_CANCELLED(3, "Refund the customer, or reinstate and fulfil the order."),
    /** The order is paid (and shipped) but the money never arrived: a direct loss. */
    UNPAID_BUT_FULFILLED(3, "Stop fulfilment and contact the customer; do not ship."),
    /** The payment was refunded but the order is still active: goods for free. */
    REFUNDED_BUT_ACTIVE(2, "Cancel the order or recover the goods."),
    /** The money is captured, but the merchant still waits for payment: the merchant missed the event. */
    PAID_BUT_AWAITING(1, "Mark the order paid; check why the merchant did not receive Concord's event."),
    /** Order amount and captured amount differ. */
    AMOUNT_MISMATCH(2, "Check the order total against the captured amount."),
    /** Concord's own state differs from the gateway's: handed to the resolver to fix. */
    CONCORD_DRIFT(1, "The resolver re-checks the payment against the status API.");

    private final int weight;
    private final String suggestedAction;

    DivergenceType(int weight, String suggestedAction) {
        this.weight = weight;
        this.suggestedAction = suggestedAction;
    }

    public int weight() {
        return weight;
    }

    public String suggestedAction() {
        return suggestedAction;
    }
}
