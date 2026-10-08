package dev.concord.ingest.aws;

import dev.concord.ingest.dedup.DedupStore;
import dev.concord.ingest.dedup.DedupStoreContract;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class DynamoDbDedupStoreTest extends DedupStoreContract {

    private static DynamoDbClient dynamo;
    private static DynamoDbDedupStore store;

    @BeforeAll
    static void startDynamo() {
        dynamo = DynamoDbLocal.client();
        DynamoDbLocal.createDedupTable(dynamo, "concord-dedup-test");
        store = new DynamoDbDedupStore(dynamo, "concord-dedup-test", LEASE, TTL);
    }

    @AfterAll
    static void stop() {
        dynamo.close();
    }

    @Override
    protected DedupStore store() {
        return store;
    }
}
