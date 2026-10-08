package dev.concord.ingest.verify;

import java.time.Duration;
import java.time.Instant;

final class Timestamps {

    private Timestamps() {
    }

    /** Rejects signatures that are too old (replay) or too far in the future (clock abuse). */
    static void requireFresh(Instant signedAt, Instant now, Duration tolerance) {
        Duration skew = Duration.between(signedAt, now).abs();
        if (skew.compareTo(tolerance) > 0) {
            throw new InvalidSignatureException("timestamp outside tolerance (" + skew.toSeconds() + "s)");
        }
    }
}
