package dev.concord.ingest.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.ingest.Crypto;
import dev.concord.ingest.queue.EventQueue;
import dev.concord.ingest.queue.IngestedEvent;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * SQS FIFO. MessageGroupId = gateway#paymentId gives per-payment order with parallelism across
 * payments; MessageDeduplicationId = sha256(dedupKey) makes a re-send within 5 minutes a no-op.
 */
public final class SqsEventQueue implements EventQueue {

    private final SqsClient sqs;
    private final String queueUrl;
    private final ObjectMapper json;

    public SqsEventQueue(SqsClient sqs, String queueUrl, ObjectMapper json) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.json = json;
    }

    @Override
    public void send(IngestedEvent event) {
        String body;
        try {
            body = json.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
        sqs.sendMessage(b -> b
                .queueUrl(queueUrl)
                .messageBody(body)
                .messageGroupId(event.messageGroupId())
                .messageDeduplicationId(Crypto.sha256Hex(event.dedupKey())));
    }
}
