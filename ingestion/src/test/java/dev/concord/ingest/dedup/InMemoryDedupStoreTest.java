package dev.concord.ingest.dedup;

class InMemoryDedupStoreTest extends DedupStoreContract {

    private final InMemoryDedupStore store = new InMemoryDedupStore(LEASE, TTL);

    @Override
    protected DedupStore store() {
        return store;
    }
}
