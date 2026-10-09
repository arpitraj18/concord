package dev.concord.engine.replay;

import java.util.function.BiConsumer;

/** Where archived raw deliveries are read from (S3 in AWS, memory in tests). */
public interface ArchiveSource {

    /** Calls {@code consumer} with (key, bytes) for every object under {@code prefix}. */
    void forEach(String prefix, BiConsumer<String, byte[]> consumer);
}
