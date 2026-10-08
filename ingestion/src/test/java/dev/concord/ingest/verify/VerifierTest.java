package dev.concord.ingest.verify;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.concord.ingest.Gateway;
import dev.concord.ingest.InboundWebhook;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Expected signatures come from openssl, not from any code in this repo:
 * <pre>
 * printf '%s' '{"a":1}'               | openssl dgst -sha256 -hmac secret -hex
 * printf '%s' '1700000000000{"a":1}'  | openssl dgst -sha256 -hmac secret -binary | base64
 * printf '%s' '1700000000.{"a":1}'    | openssl dgst -sha256 -hmac secret -hex
 * </pre>
 */
class VerifierTest {

    static final byte[] BODY = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
    static final byte[] TAMPERED = "{\"a\":2}".getBytes(StandardCharsets.UTF_8);
    static final String A_SIG = "aa9e2e3575f5d7098b6caccd790888c36d5fdb63342a73bada2d6a51747a8494";
    static final String B_SIG = "0pM2GdttJSewPFhxsH1nVKvgeUeQD/mx7t0F5acVYpI=";
    static final String C_SIG = "49f24e537407743fa4a0242bb63b94b9a47ee99cbbe071ccd8a22550ae411686";
    static final Instant SIGNED = Instant.ofEpochMilli(1_700_000_000_000L);
    static final Duration TOLERANCE = Duration.ofMinutes(5);

    static InboundWebhook hook(Gateway g, Map<String, String> headers, byte[] body, Instant receivedAt) {
        return new InboundWebhook(g, headers, body, receivedAt);
    }

    @Test
    void gatewayA() {
        var v = new GatewayAVerifier(Secrets.of("secret"));
        assertThatCode(() -> v.verify(hook(Gateway.A, Map.of("X-GwA-Signature", A_SIG), BODY, SIGNED))).doesNotThrowAnyException();
        assertThatThrownBy(() -> v.verify(hook(Gateway.A, Map.of("X-GwA-Signature", A_SIG), TAMPERED, SIGNED)))
                .hasMessageContaining("mismatch");
        assertThatThrownBy(() -> v.verify(hook(Gateway.A, Map.of(), BODY, SIGNED))).hasMessageContaining("missing");
        assertThatThrownBy(() -> v.verify(hook(Gateway.A, Map.of("X-GwA-Signature", "zz"), BODY, SIGNED)))
                .hasMessageContaining("not hex");
    }

    @Test
    void gatewayAAcceptsAnyActiveSecretDuringRotation() {
        var v = new GatewayAVerifier(Secrets.parse("new_secret, secret"));
        assertThatCode(() -> v.verify(hook(Gateway.A, Map.of("x-gwa-signature", A_SIG), BODY, SIGNED))).doesNotThrowAnyException();
        var wrong = new GatewayAVerifier(Secrets.of("other"));
        assertThatThrownBy(() -> wrong.verify(hook(Gateway.A, Map.of("x-gwa-signature", A_SIG), BODY, SIGNED)))
                .isInstanceOf(InvalidSignatureException.class);
    }

    @Test
    void gatewayB() {
        var v = new GatewayBVerifier(Secrets.of("secret"), TOLERANCE);
        Map<String, String> h = Map.of("x-gwb-signature", B_SIG, "x-gwb-timestamp", "1700000000000");
        assertThatCode(() -> v.verify(hook(Gateway.B, h, BODY, SIGNED.plusSeconds(10)))).doesNotThrowAnyException();
        assertThatThrownBy(() -> v.verify(hook(Gateway.B, h, TAMPERED, SIGNED))).hasMessageContaining("mismatch");
        Map<String, String> otherTime = Map.of("x-gwb-signature", B_SIG, "x-gwb-timestamp", "1700000000001");
        assertThatThrownBy(() -> v.verify(hook(Gateway.B, otherTime, BODY, SIGNED)))
                .as("the timestamp is part of what is signed").hasMessageContaining("mismatch");
    }

    @Test
    void gatewayBRejectsReplayOfOldWebhook() {
        var v = new GatewayBVerifier(Secrets.of("secret"), TOLERANCE);
        Map<String, String> h = Map.of("x-gwb-signature", B_SIG, "x-gwb-timestamp", "1700000000000");
        assertThatThrownBy(() -> v.verify(hook(Gateway.B, h, BODY, SIGNED.plus(Duration.ofMinutes(6)))))
                .hasMessageContaining("tolerance");
    }

    @Test
    void gatewayC() {
        var v = new GatewayCVerifier(Secrets.of("secret"), TOLERANCE);
        Map<String, String> h = Map.of("GwC-Signature", "t=1700000000,v1=" + C_SIG);
        assertThatCode(() -> v.verify(hook(Gateway.C, h, BODY, SIGNED))).doesNotThrowAnyException();
        assertThatThrownBy(() -> v.verify(hook(Gateway.C, h, TAMPERED, SIGNED))).hasMessageContaining("mismatch");
        assertThatThrownBy(() -> v.verify(hook(Gateway.C, h, BODY, SIGNED.plus(Duration.ofHours(1)))))
                .hasMessageContaining("tolerance");
        assertThatThrownBy(() -> v.verify(hook(Gateway.C, Map.of("GwC-Signature", "v1=" + C_SIG), BODY, SIGNED)))
                .hasMessageContaining("t=");
    }

    @Test
    void gatewayCAcceptsAnyOfSeveralV1Values() {
        var v = new GatewayCVerifier(Secrets.of("secret"), TOLERANCE);
        Map<String, String> h = Map.of("gwc-signature", "t=1700000000,v1=" + "00".repeat(32) + ",v1=" + C_SIG);
        assertThatCode(() -> v.verify(hook(Gateway.C, h, BODY, SIGNED))).doesNotThrowAnyException();
    }
}
