package dev.concord.ingest.archive;

/** Append-only store of raw deliveries. Must succeed before ingestion acknowledges a webhook. */
public interface RawArchive {

    /** Stores the object under {@code key}. Keys are unique per delivery, so writes never overwrite. */
    void put(String key, byte[] json);
}
