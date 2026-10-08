package dev.concord.ingest.verify;

import dev.concord.ingest.Crypto;
import dev.concord.ingest.InboundWebhook;
import java.util.HexFormat;

/**
 * Gateway A: {@code X-GwA-Signature = hex(HMAC-SHA256(secret, body))}.
 *
 * <p>The scheme has no timestamp, so a captured webhook could be replayed. That is acceptable here
 * only because replays are duplicates and the pipeline is idempotent; it is noted as a weakness.
 */
public final class GatewayAVerifier implements SignatureVerifier {

    static final String HEADER = "x-gwa-signature";

    private final Secrets secrets;

    public GatewayAVerifier(Secrets secrets) {
        this.secrets = secrets;
    }

    @Override
    public void verify(InboundWebhook webhook) {
        String given = webhook.header(HEADER);
        if (given == null) {
            throw new InvalidSignatureException("missing " + HEADER);
        }
        byte[] givenBytes;
        try {
            givenBytes = HexFormat.of().parseHex(given.trim());
        } catch (IllegalArgumentException e) {
            throw new InvalidSignatureException(HEADER + " is not hex");
        }
        for (String secret : secrets.values()) {
            if (Crypto.constantTimeEquals(Crypto.hmacSha256(secret, webhook.body()), givenBytes)) {
                return;
            }
        }
        throw new InvalidSignatureException("signature mismatch");
    }
}
