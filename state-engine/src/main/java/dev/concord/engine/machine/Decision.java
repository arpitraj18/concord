package dev.concord.engine.machine;

import java.util.List;

/**
 * What the state machine does with one claimed state, given the current one.
 *
 * @param path for {@link Kind#APPLY}: every state passed through, current and claimed included. More
 *             than two entries means webhooks for the states in between never arrived (they were
 *             dropped or are still late) and the intermediate states are inferred.
 */
public record Decision(Kind kind, CanonicalState from, CanonicalState to, List<CanonicalState> path) {

    public enum Kind {
        /** Claimed state is later on a legal path: move there. */
        APPLY,
        /** Already in the claimed state: nothing to do (e.g. a duplicate sent with a fresh id). */
        SAME,
        /** Claimed state is earlier than the current one: a late webhook. Recorded, not applied. */
        STALE,
        /**
         * Claimed state is on a different branch (FAILED vs captured): the gateway contradicted
         * itself. Not applied; the payment is flagged for verification against the status API.
         */
        CONTRADICTION
    }

    public boolean changesState() {
        return kind == Kind.APPLY;
    }

    public List<CanonicalState> inferred() {
        return path.size() <= 2 ? List.of() : path.subList(1, path.size() - 1);
    }
}
