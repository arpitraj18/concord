package dev.concord.engine.machine;

import java.util.List;

/**
 * Decides what one claim means. Pure and stateless.
 *
 * <p>Timestamps are never used: clocks across gateways and servers disagree. Order comes only from the
 * lifecycle itself. A claim is applied if it lies <em>ahead</em> on a legal path, even several steps
 * ahead, because the webhooks in between are often dropped and waiting for them would leave payments
 * stuck. A claim behind the current state is a late webhook and is ignored. A claim on another branch
 * is a contradiction and is never applied.
 */
public final class StateMachine {

    private StateMachine() {
    }

    public static Decision decide(CanonicalState current, CanonicalState claimed) {
        if (claimed == current) {
            return new Decision(Decision.Kind.SAME, current, current, List.of(current));
        }
        if (current.precedes(claimed)) {
            return new Decision(Decision.Kind.APPLY, current, claimed, current.pathTo(claimed));
        }
        if (claimed.precedes(current)) {
            return new Decision(Decision.Kind.STALE, current, current, List.of(current));
        }
        return new Decision(Decision.Kind.CONTRADICTION, current, current, List.of(current));
    }
}
