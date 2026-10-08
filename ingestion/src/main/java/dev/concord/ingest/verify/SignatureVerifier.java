package dev.concord.ingest.verify;

import dev.concord.ingest.InboundWebhook;

/**
 * Proves a webhook came from the gateway and was not altered.
 *
 * <p>Each implementation is written from the gateway's documented scheme, independently of the
 * simulator's signing code. If both sides shared one implementation, a bug in it would pass on both.
 */
public interface SignatureVerifier {

    /** @throws InvalidSignatureException if the webhook must not be trusted */
    void verify(InboundWebhook webhook);
}
