package dev.concord.simulator.dialect;

import static org.assertj.core.api.Assertions.assertThat;

import dev.concord.simulator.crypto.Hmac;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Expected values were produced with openssl, not with this codebase, e.g.
 * {@code printf '%s' '{"a":1}' | openssl dgst -sha256 -hmac secret -hex}.
 */
class SignatureTest {

    private static final byte[] BODY = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
    private static final Instant SENT_AT = Instant.ofEpochMilli(1_700_000_000_000L);

    @Test
    void hmacMatchesOpenssl() {
        assertThat(Hmac.sha256Hex("secret", BODY))
                .isEqualTo("aa9e2e3575f5d7098b6caccd790888c36d5fdb63342a73bada2d6a51747a8494");
    }

    @Test
    void gatewayASignsRawBodyAsHex() {
        Map<String, String> h = new GatewayADialect().webhookHeaders("secret", BODY, "evt_1", SENT_AT);
        assertThat(h).containsEntry(GatewayADialect.SIGNATURE_HEADER,
                "aa9e2e3575f5d7098b6caccd790888c36d5fdb63342a73bada2d6a51747a8494");
        assertThat(h).containsEntry(GatewayADialect.EVENT_ID_HEADER, "evt_1");
    }

    @Test
    void gatewayBSignsTimestampPlusBodyAsBase64() {
        Map<String, String> h = new GatewayBDialect().webhookHeaders("secret", BODY, "ignored", SENT_AT);
        assertThat(h).containsEntry(GatewayBDialect.TIMESTAMP_HEADER, "1700000000000");
        assertThat(h).containsEntry(GatewayBDialect.SIGNATURE_HEADER, "0pM2GdttJSewPFhxsH1nVKvgeUeQD/mx7t0F5acVYpI=");
        assertThat(h).doesNotContainKey("X-GwA-Event-Id");
    }

    @Test
    void gatewayCSignsTimestampDotBody() {
        Map<String, String> h = new GatewayCDialect().webhookHeaders("secret", BODY, "evt_1", SENT_AT);
        assertThat(h).containsEntry(GatewayCDialect.SIGNATURE_HEADER,
                "t=1700000000,v1=49f24e537407743fa4a0242bb63b94b9a47ee99cbbe071ccd8a22550ae411686");
    }

    @Test
    void wrongSecretGivesDifferentSignature() {
        assertThat(Hmac.sha256Hex("secret2", BODY)).isNotEqualTo(Hmac.sha256Hex("secret", BODY));
    }
}
