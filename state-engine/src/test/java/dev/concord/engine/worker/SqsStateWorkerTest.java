package dev.concord.engine.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.StateEngine;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.InMemoryPaymentStore;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.aws.ElasticMq;
import dev.concord.ingest.aws.SqsEventQueue;
import dev.concord.ingest.queue.IngestedEvent;
import java.time.Clock;
import org.junit.jupiter.api.Test;

class SqsStateWorkerTest {

    static IngestedEvent event(String paymentId, String eventId, String type) {
        String body = "{\"event\":\"" + type + "\",\"payload\":{\"payment\":{\"entity\":{\"id\":\"" + paymentId
                + "\",\"amount\":100,\"currency\":\"INR\"}}},\"created_at\":1}";
        return new IngestedEvent(1, "A#" + eventId, "A", paymentId, type, eventId, "2026-10-09T10:00:00Z", "raw/k", body);
    }

    @Test
    void processesInOrderAndDeletesOnlyAfterCommit() throws Exception {
        ObjectMapper json = new ObjectMapper();
        try (ElasticMq sqs = ElasticMq.start()) {
            String url = sqs.createFifoQueue("worker-test.fifo");
            SqsEventQueue queue = new SqsEventQueue(sqs.client(), url, json);
            queue.send(event("pay_1", "e1", "payment.authorized"));
            queue.send(event("pay_1", "e2", "payment.captured"));
            queue.send(event("pay_2", "e3", "payment.failed"));
            queue.send(event("pay_1", "e4", "settlement.processed"));

            InMemoryPaymentStore store = new InMemoryPaymentStore();
            SqsStateWorker worker = new SqsStateWorker(sqs.client(), url,
                    new StateEngine(Normalizer.withBundledMappings(json), store, Clock.systemUTC()), json);
            int processed = 0;
            for (int i = 0; i < 10 && processed < 4; i++) {
                processed += worker.pollOnce(1);
            }

            assertThat(processed).isEqualTo(4);
            assertThat(store.find(Gateway.A, "pay_1").orElseThrow().state()).isEqualTo(CanonicalState.SETTLED);
            assertThat(store.find(Gateway.A, "pay_1").orElseThrow().version()).isEqualTo(3);
            assertThat(store.find(Gateway.A, "pay_2").orElseThrow().state()).isEqualTo(CanonicalState.FAILED);
            assertThat(worker.pollOnce(1)).as("queue is empty: everything was deleted").isZero();
        }
    }

    @Test
    void failureHoldsTheRestOfThatPaymentButNotOthers() throws Exception {
        ObjectMapper json = new ObjectMapper();
        try (ElasticMq sqs = ElasticMq.start()) {
            String url = sqs.createFifoQueue("worker-fail.fifo");
            SqsEventQueue queue = new SqsEventQueue(sqs.client(), url, json);
            IngestedEvent poison = new IngestedEvent(1, "A#bad", "A", "pay_1", "x", "bad", "2026-10-09T10:00:00Z", "raw/k", "not json");
            queue.send(poison);
            queue.send(event("pay_1", "e2", "payment.captured"));
            queue.send(event("pay_2", "e3", "payment.captured"));

            InMemoryPaymentStore store = new InMemoryPaymentStore();
            SqsStateWorker worker = new SqsStateWorker(sqs.client(), url,
                    new StateEngine(Normalizer.withBundledMappings(json), store, Clock.systemUTC()), json);
            for (int i = 0; i < 3; i++) {
                worker.pollOnce(1);
            }
            assertThat(store.find(Gateway.A, "pay_2")).as("other payments are not blocked").isPresent();
            assertThat(store.find(Gateway.A, "pay_1")).as("pay_1's later event waits behind the failure").isEmpty();
        }
    }
}
