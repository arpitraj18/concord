package dev.concord.divergence;

import static dev.concord.engine.machine.CanonicalState.CAPTURED;
import static dev.concord.engine.machine.CanonicalState.PENDING;
import static dev.concord.engine.machine.CanonicalState.REFUNDED;
import static dev.concord.engine.machine.CanonicalState.SETTLED;
import static dev.concord.engine.machine.CanonicalState.UNKNOWN;

import dev.concord.engine.machine.CanonicalState;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Pure: which divergences one observation shows. The order side uses the sample merchant's statuses
 * (AWAITING_PAYMENT, PAID, PAYMENT_FAILED, REFUNDED, CANCELLED); a real merchant's adapter maps its own
 * statuses onto these.
 *
 * <pre>
 *  money really...        order says          divergence
 *  captured / settled     CANCELLED, FAILED   PAID_BUT_CANCELLED
 *  captured / settled     AWAITING_PAYMENT    PAID_BUT_AWAITING
 *  refunded               PAID                REFUNDED_BUT_ACTIVE
 *  not captured           PAID                UNPAID_BUT_FULFILLED
 *  captured, amount A     amount B != A       AMOUNT_MISMATCH
 *  gateway X, Concord Y   (any)               CONCORD_DRIFT
 * </pre>
 */
public final class Classifier {

    private static final Set<CanonicalState> MONEY_IN = EnumSet.of(CAPTURED, SETTLED);

    private Classifier() {
    }

    public static Set<DivergenceType> classify(Observation o) {
        Set<DivergenceType> found = EnumSet.noneOf(DivergenceType.class);
        CanonicalState truth = o.truth();

        if (o.gatewayState() != null && o.gatewayState() != o.concord().state()
                && !(o.concord().state() == UNKNOWN && o.gatewayState() == PENDING)) {
            found.add(DivergenceType.CONCORD_DRIFT);
        }
        if (o.order() == null) {
            return found;
        }
        String order = o.order().status();
        if (MONEY_IN.contains(truth)) {
            if (order.equals("CANCELLED") || order.equals("PAYMENT_FAILED")) {
                found.add(DivergenceType.PAID_BUT_CANCELLED);
            } else if (order.equals("AWAITING_PAYMENT")) {
                found.add(DivergenceType.PAID_BUT_AWAITING);
            }
            if (o.order().amountMinor() != null && o.concord().amountMinor() != null
                    && !Objects.equals(o.order().amountMinor(), o.concord().amountMinor())) {
                found.add(DivergenceType.AMOUNT_MISMATCH);
            }
        } else if (truth == REFUNDED) {
            if (order.equals("PAID")) {
                found.add(DivergenceType.REFUNDED_BUT_ACTIVE);
            }
        } else if (order.equals("PAID")) {
            found.add(DivergenceType.UNPAID_BUT_FULFILLED);
        }
        return found;
    }
}
