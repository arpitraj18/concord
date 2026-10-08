package dev.concord.ingest;

import java.util.Locale;
import java.util.Optional;

/** The gateways Concord accepts webhooks from. Taken from the URL path: /webhooks/{gateway}. */
public enum Gateway {
    A,
    B,
    C;

    public static Optional<Gateway> fromPath(String segment) {
        if (segment == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(valueOf(segment.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
