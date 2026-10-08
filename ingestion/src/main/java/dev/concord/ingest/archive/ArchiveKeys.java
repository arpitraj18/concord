package dev.concord.ingest.archive;

import dev.concord.ingest.Gateway;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * S3 key layout: {@code <prefix>/gateway=A/dt=2026-10-08/hour=14/<epochMillis>-<uuid>.json}.
 * Hive-style partitions keep replays of one day or one gateway cheap, and allow Athena queries later.
 */
public final class ArchiveKeys {

    public static final String VERIFIED = "raw";
    public static final String REJECTED = "rejected";

    private ArchiveKeys() {
    }

    public static String newKey(String prefix, Gateway gateway, Instant receivedAt) {
        ZonedDateTime t = receivedAt.atZone(ZoneOffset.UTC);
        return String.format("%s/gateway=%s/dt=%04d-%02d-%02d/hour=%02d/%d-%s.json",
                prefix, gateway, t.getYear(), t.getMonthValue(), t.getDayOfMonth(), t.getHour(),
                receivedAt.toEpochMilli(), UUID.randomUUID());
    }
}
