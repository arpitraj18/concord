package dev.concord.ingest.verify;

import dev.concord.ingest.Crypto;
import dev.concord.ingest.InboundWebhook;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

/**
 * Gateway B: {@code x-gwb-signature = base64(HMAC-SHA256(secret, x-gwb-timestamp + body))}, with the
 * timestamp in epoch milliseconds. Old timestamps are rejected so captured webhooks cannot be replayed.
 */
public final class GatewayBVerifier implements SignatureVerifier {

    static final String SIGNATURE = "x-gwb-signature";
    static final String TIMESTAMP = "x-gwb-timestamp";

    private final Secrets secrets;
    private final Duration tolerance;

    public GatewayBVerifier(Secrets secrets, Duration tolerance) {
        this.secrets = secrets;
        this.tolerance = tolerance;
    }

    @Override
    public void verify(InboundWebhook webhook) {
        String signature = webhook.header(SIGNATURE);
        String timestamp = webhook.header(TIMESTAMP);
        if (signature == null || timestamp == null) {
            throw new InvalidSignatureException("missing " + SIGNATURE + " or " + TIMESTAMP);
        }
        Instant signedAt;
        try {
            signedAt = Instant.ofEpochMilli(Long.parseLong(timestamp.trim()));
        } catch (NumberFormatException e) {
            throw new InvalidSignatureException(TIMESTAMP + " is not a number");
        }
        Timestamps.requireFresh(signedAt, webhook.receivedAt(), tolerance);

        byte[] given;
        try {
            given = Base64.getDecoder().decode(signature.trim());
        } catch (IllegalArgumentException e) {
            throw new InvalidSignatureException(SIGNATURE + " is not base64");
        }
        byte[] prefix = timestamp.trim().getBytes(StandardCharsets.UTF_8);
        for (String secret : secrets.values()) {
            if (Crypto.constantTimeEquals(Crypto.hmacSha256(secret, prefix, webhook.body()), given)) {
                return;
            }
        }
        throw new InvalidSignatureException("signature mismatch");
    }
}
