package dev.concord.engine.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.ProcessResult;
import dev.concord.engine.StateEngine;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.InboundWebhook;
import dev.concord.ingest.archive.ArchiveKeys;
import dev.concord.ingest.archive.ArchivedWebhook;
import dev.concord.ingest.envelope.Envelope;
import dev.concord.ingest.envelope.EnvelopeExtractor;
import dev.concord.ingest.envelope.UnparseableWebhookException;
import dev.concord.ingest.queue.IngestedEvent;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Rebuilds payment state from the raw archive alone (event sourcing).
 *
 * <p>Every archived delivery under {@code raw/} is re-extracted with ingestion's own code and fed to the
 * engine in receive order. Duplicate deliveries are absorbed by the engine's per-event idempotency, the
 * same way ingestion's dedup absorbed them live. Signatures are not re-checked: {@code raw/} only holds
 * deliveries that passed verification, and B/C timestamps would now be outside their tolerance.
 */
public final class ArchiveReplayer {

    private final ObjectMapper json;
    private final EnvelopeExtractor extractor;

    public ArchiveReplayer(ObjectMapper json) {
        this.json = json;
        this.extractor = new EnvelopeExtractor(json);
    }

    public record Report(int deliveries, int unparseable, int processed, int redeliveries,
                         Map<dev.concord.engine.store.EventOutcome, Integer> outcomes) {
    }

    private record Item(String key, ArchivedWebhook archived) {
    }

    public Report replay(ArchiveSource source, StateEngine engine) {
        List<Item> items = new ArrayList<>();
        source.forEach(ArchiveKeys.VERIFIED + "/", (key, bytes) -> {
            try {
                items.add(new Item(key, json.readValue(bytes, ArchivedWebhook.class)));
            } catch (IOException e) {
                throw new UncheckedIOException("corrupt archive object " + key, e);
            }
        });
        items.sort(Comparator.comparing((Item i) -> Instant.parse(i.archived().receivedAt())).thenComparing(Item::key));

        int unparseable = 0;
        int redeliveries = 0;
        Map<dev.concord.engine.store.EventOutcome, Integer> outcomes = new EnumMap<>(dev.concord.engine.store.EventOutcome.class);
        for (Item item : items) {
            ArchivedWebhook a = item.archived();
            byte[] body = Base64.getDecoder().decode(a.bodyBase64());
            InboundWebhook webhook = new InboundWebhook(Gateway.valueOf(a.gateway()), a.headers(), body,
                    Instant.parse(a.receivedAt()));
            Envelope envelope;
            try {
                envelope = extractor.extract(webhook);
            } catch (UnparseableWebhookException e) {
                unparseable++;
                continue;
            }
            IngestedEvent event = new IngestedEvent(IngestedEvent.SCHEMA_VERSION, envelope.dedupKey(),
                    envelope.gateway().name(), envelope.paymentId(), envelope.eventType(), envelope.gatewayEventId(),
                    a.receivedAt(), item.key(), new String(body, StandardCharsets.UTF_8));
            ProcessResult r = engine.process(event);
            if (r.redelivery()) {
                redeliveries++;
            } else {
                outcomes.merge(r.outcome(), 1, Integer::sum);
            }
        }
        return new Report(items.size(), unparseable, items.size() - unparseable - redeliveries, redeliveries, outcomes);
    }
}
