package dev.concord.ingest.archive;

import java.util.Map;

/**
 * What is stored for every delivery, duplicates included. The body is base64 of the exact received
 * bytes, so a replay can re-verify signatures and re-parse exactly what arrived.
 *
 */
public record ArchivedWebhook(
        int schemaVersion,
        String gateway,
        String receivedAt,
        Map<String, String> headers,
        String bodyBase64) {
}
