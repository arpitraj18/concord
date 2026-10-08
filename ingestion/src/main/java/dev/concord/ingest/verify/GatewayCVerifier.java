package dev.concord.ingest.verify;

import dev.concord.ingest.Crypto;
import dev.concord.ingest.InboundWebhook;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * Gateway C: {@code GwC-Signature: t=<unix seconds>,v1=<hex>[,v1=<hex>...]} where
 * {@code v1 = hex(HMAC-SHA256(secret, t + "." + body))}. Several v1 values may be present while the
 * gateway rotates its secret; any one matching is enough.
 */
public final class GatewayCVerifier implements SignatureVerifier {

    static final String HEADER = "gwc-signature";

    private final Secrets secrets;
    private final Duration tolerance;

    public GatewayCVerifier(Secrets secrets, Duration tolerance) {
        this.secrets = secrets;
        this.tolerance = tolerance;
    }

    @Override
    public void verify(InboundWebhook webhook) {
        String header = webhook.header(HEADER);
        if (header == null) {
            throw new InvalidSignatureException("missing " + HEADER);
        }
        String t = null;
        List<byte[]> candidates = new ArrayList<>();
        for (String part : header.split(",")) {
            int eq = part.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = part.substring(0, eq).trim();
            String value = part.substring(eq + 1).trim();
            if (key.equals("t")) {
                t = value;
            } else if (key.equals("v1")) {
                try {
                    candidates.add(HexFormat.of().parseHex(value));
                } catch (IllegalArgumentException ignored) {
                    // a malformed candidate simply cannot match
                }
            }
        }
        if (t == null || candidates.isEmpty()) {
            throw new InvalidSignatureException(HEADER + " needs t= and at least one v1=");
        }
        Instant signedAt;
        try {
            signedAt = Instant.ofEpochSecond(Long.parseLong(t));
        } catch (NumberFormatException e) {
            throw new InvalidSignatureException("t is not a number");
        }
        Timestamps.requireFresh(signedAt, webhook.receivedAt(), tolerance);

        byte[] prefix = (t + ".").getBytes(StandardCharsets.UTF_8);
        for (String secret : secrets.values()) {
            byte[] expected = Crypto.hmacSha256(secret, prefix, webhook.body());
            for (byte[] candidate : candidates) {
                if (Crypto.constantTimeEquals(expected, candidate)) {
                    return;
                }
            }
        }
        throw new InvalidSignatureException("signature mismatch");
    }
}
