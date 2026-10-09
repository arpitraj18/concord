package dev.concord.engine.aws;

import dev.concord.engine.store.PaymentStore;
import dev.concord.engine.store.PaymentStoreContract;
import dev.concord.ingest.aws.DynamoDbLocal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class DynamoDbPaymentStoreTest extends PaymentStoreContract {

    private static DynamoDbClient dynamo;
    private static DynamoDbPaymentStore store;

    @BeforeAll
    static void start() {
        dynamo = DynamoDbLocal.client();
        PaymentsTable.create(dynamo, "concord-payments-test");
        store = new DynamoDbPaymentStore(dynamo, "concord-payments-test");
    }

    @AfterAll
    static void stop() {
        dynamo.close();
    }

    @Override
    protected PaymentStore store() {
        return store;
    }
}
