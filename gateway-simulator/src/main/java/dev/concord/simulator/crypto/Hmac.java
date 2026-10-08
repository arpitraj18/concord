package dev.concord.simulator.crypto;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class Hmac {

    private Hmac() {
    }

    public static byte[] sha256(String secret, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return mac.doFinal(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    public static String sha256Hex(String secret, byte[] data) {
        return HexFormat.of().formatHex(sha256(secret, data));
    }

    public static String sha256Base64(String secret, byte[] data) {
        return Base64.getEncoder().encodeToString(sha256(secret, data));
    }

    public static byte[] concat(String prefix, byte[] body) {
        byte[] p = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[p.length + body.length];
        System.arraycopy(p, 0, out, 0, p.length);
        System.arraycopy(body, 0, out, p.length, body.length);
        return out;
    }
}
