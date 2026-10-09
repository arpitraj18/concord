package dev.concord.divergence;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.resolver.merchant.MerchantOrder;

/**
 * The three views of one payment at one moment.
 *
 * @param gatewayState the status API's answer (the truth), or null if it could not be reached
 * @param order        the merchant's order, or null if the merchant does not know it or could not be reached
 */
public record Observation(PaymentRecord concord, CanonicalState gatewayState, MerchantOrder order) {

    /** What the money really did: the gateway if it answered, otherwise Concord's best knowledge. */
    public CanonicalState truth() {
        return gatewayState != null ? gatewayState : concord.state();
    }
}
