package dev.concord.merchant;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Verifies {@code Concord-Signature: t=<unix>,v1=hex(HMAC-SHA256(secret, t + "." + body))}.
 * Written from the documented scheme, sharing no code with Concord.
 */
@Component
public class ConcordSignature {

    private final byte[] secret;
    private final Duration tolerance;

    public ConcordSignature(@Value("${merchant.concord-secret}") String secret,
                            @Value("${merchant.signature-tolerance-seconds}") long toleranceSeconds) {
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
        this.tolerance = Duration.ofSeconds(toleranceSeconds);
    }

    public boolean valid(String header, byte[] body, Instant now) {
        if (header == null) {
            return false;
        }
        String t = null;
        String v1 = null;
        for (String part : header.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length == 2 && kv[0].equals("t")) {
                t = kv[1];
            } else if (kv.length == 2 && kv[0].equals("v1")) {
                v1 = kv[1];
            }
        }
        if (t == null || v1 == null) {
            return false;
        }
        try {
            if (Duration.between(Instant.ofEpochSecond(Long.parseLong(t)), now).abs().compareTo(tolerance) > 0) {
                return false;
            }
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            mac.update((t + ".").getBytes(StandardCharsets.UTF_8));
            byte[] expected = mac.doFinal(body);
            return MessageDigest.isEqual(expected, HexFormat.of().parseHex(v1));
        } catch (NumberFormatException | GeneralSecurityException e) {
            return false;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
