package dev.concord.resolver.status;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;

/** The two actions the resolver may take on an authorized-but-not-captured payment. */
public interface GatewayActions {

    /** @return the state the gateway reports after the call, or null if the call failed */
    CanonicalState capture(Gateway gateway, String paymentId);

    CanonicalState release(Gateway gateway, String paymentId);
}
