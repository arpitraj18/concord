package dev.concord.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.Events;
import dev.concord.engine.StateEngine;
import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.aws.PaymentsTable;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.ingest.Gateway;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The outbox, end to end through real DynamoDB Streams (DynamoDB Local). */
class DynamoDbStreamsOutboxReaderTest {

    @Test
    void everyCommittedOutboxEntryComesOutOfTheStreamInOrder() {
        var local = DynamoDBEmbedded.create();
        var dynamo = local.dynamoDbClient();
        PaymentsTable.create(dynamo, "outbox-stream-test");
        DynamoDbPaymentStore store = new DynamoDbPaymentStore(dynamo, "outbox-stream-test");
        StateEngine engine = new StateEngine(Normalizer.withBundledMappings(new ObjectMapper()), store, Clock.systemUTC());

        for (int i = 0; i < 20; i++) {
            engine.process(Events.claim("s" + i, CanonicalState.AUTHORIZED, "A#a" + i, 1));
            engine.process(Events.claim("s" + i, CanonicalState.AUTHORIZED, "A#dup" + i, 2)); // SAME: no outbox
            engine.process(Events.claim("s" + i, CanonicalState.CAPTURED, "A#c" + i, 3));
        }
        String arn = dynamo.describeTable(b -> b.tableName("outbox-stream-test")).table().latestStreamArn();

        List<OutboxEntry> got = new ArrayList<>();
        DynamoDbStreamsOutboxReader reader = new DynamoDbStreamsOutboxReader(local.dynamoDbStreamsClient(), arn, got::add);
        for (int i = 0; i < 5 && got.size() < 40; i++) {
            reader.pollOnce();
        }
        assertThat(got).hasSize(40);
        for (int i = 0; i < 20; i++) {
            String id = "s" + i;
            assertThat(got.stream().filter(e -> e.paymentId().equals(id)).map(OutboxEntry::toState).toList())
                    .containsExactly(CanonicalState.AUTHORIZED, CanonicalState.CAPTURED);
            assertThat(got.stream().filter(e -> e.paymentId().equals(id)).toList())
                    .isEqualTo(store.outbox(Gateway.A, id));
        }
        local.shutdown();
    }
}
