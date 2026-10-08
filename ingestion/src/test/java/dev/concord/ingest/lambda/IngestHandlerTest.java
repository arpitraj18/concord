package dev.concord.ingest.lambda;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import dev.concord.ingest.Crypto;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.Wiring;
import dev.concord.ingest.archive.InMemoryRawArchive;
import dev.concord.ingest.dedup.InMemoryDedupStore;
import dev.concord.ingest.queue.InMemoryFifoQueue;
import dev.concord.ingest.verify.Secrets;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IngestHandlerTest {

    final Clock clock = Clock.systemUTC();
    final InMemoryFifoQueue queue = new InMemoryFifoQueue(clock);
    final IngestHandler handler = new IngestHandler(Wiring.service(
            new IngestConfig(Map.of(Gateway.A, Secrets.of("s")), Duration.ofMinutes(5), Duration.ofSeconds(30),
                    Duration.ofDays(7), null, null, null),
            new InMemoryRawArchive(), new InMemoryDedupStore(Duration.ofSeconds(30), Duration.ofDays(7)), queue), clock);

    static final String BODY = "{\"event\":\"payment.captured\",\"payload\":{\"payment\":{\"entity\":{\"id\":\"pay_1\"}}}}";

    APIGatewayV2HTTPEvent event(String gateway, String body, boolean base64) {
        String sig = HexFormat.of().formatHex(Crypto.hmacSha256("s", BODY.getBytes(StandardCharsets.UTF_8)));
        return APIGatewayV2HTTPEvent.builder()
                .withPathParameters(Map.of("gateway", gateway))
                .withHeaders(Map.of("x-gwa-signature", sig, "x-gwa-event-id", "evt_1"))
                .withBody(body)
                .withIsBase64Encoded(base64)
                .build();
    }

    @Test
    void plainTextBody() {
        APIGatewayV2HTTPResponse r = handler.handleRequest(event("a", BODY, false), null);
        assertThat(r.getStatusCode()).isEqualTo(200);
        assertThat(r.getBody()).contains("ACCEPTED");
        assertThat(queue.messages()).hasSize(1);
    }

    @Test
    void base64BodyIsDecodedBeforeVerification() {
        String encoded = Base64.getEncoder().encodeToString(BODY.getBytes(StandardCharsets.UTF_8));
        assertThat(handler.handleRequest(event("A", encoded, true), null).getStatusCode()).isEqualTo(200);
    }

    @Test
    void unknownGatewayIs404() {
        assertThat(handler.handleRequest(event("zz", BODY, false), null).getStatusCode()).isEqualTo(404);
    }
}
