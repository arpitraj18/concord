package dev.concord.ingest.queue;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.ingest.aws.ElasticMq;
import dev.concord.ingest.aws.SqsEventQueue;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

/** Against ElasticMQ, an SQS-compatible server that also implements FIFO groups and deduplication. */
class SqsEventQueueTest {

    static ElasticMq sqs;

    @BeforeAll
    static void start() {
        sqs = ElasticMq.start();
    }

    @AfterAll
    static void stop() {
        sqs.close();
    }

    static IngestedEvent event(String dedupKey, String paymentId) {
        return new IngestedEvent(1, dedupKey, "A", paymentId, "payment.captured", "evt", "2026-10-08T12:00:00Z",
                "raw/x.json", "{}");
    }

    @Test
    void sendsWithGroupIdAndDropsResendOfSameEvent() throws Exception {
        String url = sqs.createFifoQueue("ingest-test.fifo");
        ObjectMapper json = new ObjectMapper();
        SqsEventQueue queue = new SqsEventQueue(sqs.client(), url, json);

        queue.send(event("A#evt_1", "pay_1"));
        queue.send(event("A#evt_1", "pay_1"));
        queue.send(event("A#evt_2", "pay_2"));

        List<Message> messages = sqs.client().receiveMessage(b -> b.queueUrl(url).maxNumberOfMessages(10)
                .messageSystemAttributeNames(MessageSystemAttributeName.MESSAGE_GROUP_ID)).messages();
        assertThat(messages).hasSize(2);
        assertThat(messages).extracting(m -> m.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID))
                .containsExactlyInAnyOrder("A#pay_1", "A#pay_2");
        IngestedEvent roundTrip = json.readValue(messages.getFirst().body(), IngestedEvent.class);
        assertThat(roundTrip.schemaVersion()).isEqualTo(1);
    }
}
