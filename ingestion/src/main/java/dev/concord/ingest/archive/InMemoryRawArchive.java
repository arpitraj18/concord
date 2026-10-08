package dev.concord.ingest.archive;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public final class InMemoryRawArchive implements RawArchive {

    private final Map<String, byte[]> objects = new ConcurrentSkipListMap<>();

    @Override
    public void put(String key, byte[] json) {
        if (objects.putIfAbsent(key, json.clone()) != null) {
            throw new IllegalStateException("archive key reused: " + key);
        }
    }

    public Map<String, byte[]> objects() {
        return Map.copyOf(objects);
    }

    public int size() {
        return objects.size();
    }
}
