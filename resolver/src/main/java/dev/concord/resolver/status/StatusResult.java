package dev.concord.resolver.status;

import dev.concord.engine.machine.CanonicalState;

/** What one status API call told us. */
public sealed interface StatusResult {

    /** The gateway reports {@code state}; {@code raw} is the gateway's own status text, for audits. */
    record Found(CanonicalState state, String raw) implements StatusResult {
    }

    /** The gateway does not know the payment (yet). */
    record NotFound() implements StatusResult {
    }

    /** No answer: outage, timeout, circuit open, rate limited, or a status no mapping rule covers. */
    record Unavailable(String reason) implements StatusResult {
    }
}
