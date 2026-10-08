package dev.concord.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import dev.concord.ingest.archive.InMemoryRawArchive;
import dev.concord.ingest.archive.RawArchive;
import dev.concord.ingest.dedup.InMemoryDedupStore;
import dev.concord.ingest.queue.EventQueue;
import dev.concord.ingest.queue.InMemoryFifoQueue;
import dev.concord.ingest.queue.IngestedEvent;
import dev.concord.ingest.verify.Secrets;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class IngestionServiceTest {

    static final String SECRET = "whsec_test";
    static final Instant NOW = Instant.parse("2026-10-08T12:00:00Z");
    static final Duration LEASE = Duration.ofSeconds(30);

    final MutableClock clock = new MutableClock(NOW);
    final InMemoryRawArchive archive = new InMemoryRawArchive();
    final InMemoryDedupStore dedup = new InMemoryDedupStore(LEASE, Duration.ofDays(7));
    final InMemoryFifoQueue queue = new InMemoryFifoQueue(clock);
    final IngestConfig config = new IngestConfig(Map.of(Gateway.A, Secrets.of(SECRET)), Duration.ofMinutes(5),
            LEASE, Duration.ofDays(7), null, null, null);

    IngestionService service(RawArchive archive, EventQueue queue) {
        return Wiring.service(config, archive, dedup, queue);
    }

    InboundWebhook captured(String eventId) {
        String body = "{\"event\":\"payment.captured\",\"payload\":{\"payment\":{\"entity\":{\"id\":\"pay_1\"}}}}";
        return signed(eventId, body);
    }

    InboundWebhook signed(String eventId, String body) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        String sig = HexFormat.of().formatHex(Crypto.hmacSha256(SECRET, bytes));
        return new InboundWebhook(Gateway.A, Map.of("X-GwA-Signature", sig, "X-GwA-Event-Id", eventId), bytes, clock.instant());
    }

    @Test
    void acceptsOnceAndRecognisesDuplicates() {
        IngestionService s = service(archive, queue);
        assertThat(s.ingest(captured("evt_1"))).isEqualTo(IngestResult.ACCEPTED);
        assertThat(s.ingest(captured("evt_1"))).isEqualTo(IngestResult.DUPLICATE);
        assertThat(s.ingest(captured("evt_2"))).as("same meaning, new id: passed on, state engine absorbs it")
                .isEqualTo(IngestResult.ACCEPTED);

        assertThat(queue.messages()).extracting(IngestedEvent::gatewayEventId).containsExactly("evt_1", "evt_2");
        assertThat(archive.size()).as("every delivery is archived, duplicates included").isEqualTo(3);
        IngestedEvent first = queue.messages().getFirst();
        assertThat(first.messageGroupId()).isEqualTo("A#pay_1");
        assertThat(first.archiveKey()).startsWith("raw/gateway=A/dt=2026-10-08/hour=12/");
    }

    @Test
    void badSignatureIsRejectedAndArchivedSeparately() {
        IngestionService s = service(archive, queue);
        InboundWebhook good = captured("evt_1");
        InboundWebhook forged = new InboundWebhook(Gateway.A, Map.of("X-GwA-Signature", "00".repeat(32),
                "X-GwA-Event-Id", "evt_1"), good.body(), NOW);
        assertThat(s.ingest(forged)).isEqualTo(IngestResult.REJECTED);
        assertThat(queue.messages()).isEmpty();
        assertThat(archive.objects().keySet()).allMatch(k -> k.startsWith("rejected/"));
    }

    @Test
    void gatewayWithoutSecretIsUnknown() {
        var b = new InboundWebhook(Gateway.B, Map.of(), new byte[0], NOW);
        assertThat(service(archive, queue).ingest(b)).isEqualTo(IngestResult.UNKNOWN_GATEWAY);
    }

    @Test
    void authenticButUnparseableIsArchivedAndAcknowledged() {
        IngestionService s = service(archive, queue);
        assertThat(s.ingest(signed("evt_x", "{\"event\":\"something.new\"}"))).isEqualTo(IngestResult.UNPARSEABLE);
        assertThat(archive.size()).isEqualTo(1);
        assertThat(queue.messages()).isEmpty();
    }

    @Test
    void archiveFailureMeansNothingIsAcknowledgedOrClaimed() {
        RawArchive broken = (k, v) -> {
            throw new IllegalStateException("S3 down");
        };
        assertThat(service(broken, queue).ingest(captured("evt_1"))).isEqualTo(IngestResult.ERROR);
        assertThat(dedup.size()).isZero();
        assertThat(service(archive, queue).ingest(captured("evt_1"))).as("gateway retry succeeds").isEqualTo(IngestResult.ACCEPTED);
    }

    @Test
    void queueFailureReleasesClaimSoTheRetryGoesThrough() {
        AtomicBoolean sqsDown = new AtomicBoolean(true);
        EventQueue flaky = e -> {
            if (sqsDown.get()) {
                throw new IllegalStateException("SQS down");
            }
            queue.send(e);
        };
        IngestionService s = service(archive, flaky);
        assertThat(s.ingest(captured("evt_1"))).isEqualTo(IngestResult.ERROR);
        sqsDown.set(false);
        assertThat(s.ingest(captured("evt_1"))).isEqualTo(IngestResult.ACCEPTED);
        assertThat(queue.messages()).hasSize(1);
    }

    @Test
    void crashBetweenClaimAndEnqueueIsRecoveredAfterLease() {
        // Simulates an instance that claimed the event and died before enqueueing it.
        dedup.claim("A#evt_1", "dead-instance", NOW);
        IngestionService s = service(archive, queue);

        assertThat(s.ingest(captured("evt_1"))).as("within lease").isEqualTo(IngestResult.IN_FLIGHT);
        clock.advance(LEASE.plusSeconds(1));
        assertThat(s.ingest(captured("evt_1"))).as("after lease").isEqualTo(IngestResult.ACCEPTED);
        assertThat(s.ingest(captured("evt_1"))).isEqualTo(IngestResult.DUPLICATE);
        assertThat(queue.messages()).hasSize(1);
    }

    @Test
    void reSendAfterLostMarkIsAbsorbedByQueueDedupWindow() {
        // If markEnqueued is lost, a later retry takes over the stale claim and sends again.
        // The queue's dedup id (= dedup key) drops that re-send within its 5-minute window.
        service(archive, queue).ingest(captured("evt_1"));
        queue.send(queue.messages().getFirst());
        assertThat(queue.sendCalls()).isEqualTo(2);
        assertThat(queue.messages()).hasSize(1);
    }

    static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }
}
