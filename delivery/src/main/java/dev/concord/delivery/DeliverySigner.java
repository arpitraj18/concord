package dev.concord.delivery;

import dev.concord.ingest.Crypto;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;

/**
 * Concord signs what it sends, exactly like a gateway does:
 * {@code Concord-Signature: t=<unix>,v1=hex(HMAC-SHA256(secret, t + "." + body))}. The merchant
 * verifies it and rejects old timestamps.
 */
public final class DeliverySigner {

    public static final String SIGNATURE_HEADER = "Concord-Signature";
    public static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    private final String secret;

    public DeliverySigner(String secret) {
        this.secret = secret;
    }

    public Map<String, String> headers(byte[] body, String idempotencyKey, Instant now) {
        String t = Long.toString(now.getEpochSecond());
        String v1 = HexFormat.of().formatHex(Crypto.hmacSha256(secret, (t + ".").getBytes(StandardCharsets.UTF_8), body));
        return Map.of(SIGNATURE_HEADER, "t=" + t + ",v1=" + v1, IDEMPOTENCY_HEADER, idempotencyKey);
    }
}
