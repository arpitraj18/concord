package dev.concord.ingest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.ingest.archive.ArchiveKeys;
import dev.concord.ingest.archive.ArchivedWebhook;
import dev.concord.ingest.archive.RawArchive;
import dev.concord.ingest.dedup.ClaimOutcome;
import dev.concord.ingest.dedup.DedupStore;
import dev.concord.ingest.envelope.Envelope;
import dev.concord.ingest.envelope.EnvelopeExtractor;
import dev.concord.ingest.envelope.UnparseableWebhookException;
import dev.concord.ingest.queue.EventQueue;
import dev.concord.ingest.queue.IngestedEvent;
import dev.concord.ingest.verify.InvalidSignatureException;
import dev.concord.ingest.verify.SignatureVerifier;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * verify -> archive -> claim -> enqueue -> mark enqueued -> acknowledge.
 *
 * <p>The webhook is acknowledged only after it is durably archived and either enqueued or known to be
 * a duplicate. Anything less and a crash could lose it after the gateway was told "got it". The work is
 * kept minimal (no state logic) so the response is fast and gateways do not time out and retry.
 */
public final class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);
    private static final Set<String> NOT_ARCHIVED_HEADERS = Set.of("authorization", "cookie");

    private final Map<Gateway, SignatureVerifier> verifiers;
    private final EnvelopeExtractor extractor;
    private final RawArchive archive;
    private final DedupStore dedup;
    private final EventQueue queue;
    private final ObjectMapper json;

    public IngestionService(Map<Gateway, SignatureVerifier> verifiers, EnvelopeExtractor extractor, RawArchive archive,
                            DedupStore dedup, EventQueue queue, ObjectMapper json) {
        this.verifiers = Map.copyOf(verifiers);
        this.extractor = extractor;
        this.archive = archive;
        this.dedup = dedup;
        this.queue = queue;
        this.json = json;
    }

    public IngestResult ingest(InboundWebhook webhook) {
        SignatureVerifier verifier = verifiers.get(webhook.gateway());
        if (verifier == null) {
            return IngestResult.UNKNOWN_GATEWAY;
        }
        try {
            verifier.verify(webhook);
        } catch (InvalidSignatureException e) {
            log.warn("rejected webhook from gateway {}: {}", webhook.gateway(), e.getMessage());
            tryArchive(ArchiveKeys.REJECTED, webhook);
            return IngestResult.REJECTED;
        }

        String archiveKey;
        try {
            archiveKey = archive(ArchiveKeys.VERIFIED, webhook);
        } catch (RuntimeException e) {
            log.error("archive failed for gateway {}; not acknowledging", webhook.gateway(), e);
            return IngestResult.ERROR;
        }

        Envelope envelope;
        try {
            envelope = extractor.extract(webhook);
        } catch (UnparseableWebhookException e) {
            log.warn("unparseable webhook archived at {}: {}", archiveKey, e.getMessage());
            return IngestResult.UNPARSEABLE;
        }

        String token = UUID.randomUUID().toString();
        ClaimOutcome claim;
        try {
            claim = dedup.claim(envelope.dedupKey(), token, webhook.receivedAt());
        } catch (RuntimeException e) {
            log.error("dedup store failed for {}", envelope.dedupKey(), e);
            return IngestResult.ERROR;
        }
        switch (claim) {
            case DUPLICATE -> {
                return IngestResult.DUPLICATE;
            }
            case IN_FLIGHT -> {
                return IngestResult.IN_FLIGHT;
            }
            case TAKEN_OVER -> log.warn("taking over abandoned claim for {}", envelope.dedupKey());
            case CLAIMED -> {
            }
        }

        IngestedEvent event = new IngestedEvent(IngestedEvent.SCHEMA_VERSION, envelope.dedupKey(),
                envelope.gateway().name(), envelope.paymentId(), envelope.eventType(), envelope.gatewayEventId(),
                webhook.receivedAt().toString(), archiveKey, new String(webhook.body(), StandardCharsets.UTF_8));
        try {
            queue.send(event);
        } catch (RuntimeException e) {
            log.error("enqueue failed for {}; releasing claim", envelope.dedupKey(), e);
            try {
                dedup.release(envelope.dedupKey(), token);
            } catch (RuntimeException releaseFailure) {
                log.error("release failed for {}; claim will expire after its lease", envelope.dedupKey(), releaseFailure);
            }
            return IngestResult.ERROR;
        }

        try {
            dedup.markEnqueued(envelope.dedupKey(), token, webhook.receivedAt());
        } catch (RuntimeException e) {
            // The event IS enqueued. If a retry later takes over the stale claim, the queue's own
            // dedup id absorbs the re-send within its window, and the state engine is idempotent beyond it.
            log.warn("enqueued {} but could not mark it; a later retry may re-send", envelope.dedupKey(), e);
        }
        return IngestResult.ACCEPTED;
    }

    private void tryArchive(String prefix, InboundWebhook webhook) {
        try {
            archive(prefix, webhook);
        } catch (RuntimeException e) {
            log.warn("could not archive rejected webhook", e);
        }
    }

    private String archive(String prefix, InboundWebhook webhook) {
        String key = ArchiveKeys.newKey(prefix, webhook.gateway(), webhook.receivedAt());
        Map<String, String> headers = webhook.headers().entrySet().stream()
                .filter(e -> !NOT_ARCHIVED_HEADERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ArchivedWebhook record = new ArchivedWebhook(1, webhook.gateway().name(), webhook.receivedAt().toString(),
                headers, Base64.getEncoder().encodeToString(webhook.body()));
        try {
            archive.put(key, json.writeValueAsBytes(record));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
        return key;
    }
}
