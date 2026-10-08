package dev.concord.ingest;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * A webhook exactly as it arrived. {@code body} is kept as raw bytes because signatures are computed
 * over the exact bytes; parsing and re-serialising JSON would break them.
 */
public record InboundWebhook(Gateway gateway, Map<String, String> headers, byte[] body, Instant receivedAt) {

    public InboundWebhook {
        Map<String, String> lower = new TreeMap<>();
        headers.forEach((k, v) -> lower.put(k.toLowerCase(Locale.ROOT), v));
        headers = Map.copyOf(lower);
    }

    /** Header lookup is case-insensitive, as HTTP requires. */
    public String header(String name) {
        return headers.get(name.toLowerCase(Locale.ROOT));
    }
}
