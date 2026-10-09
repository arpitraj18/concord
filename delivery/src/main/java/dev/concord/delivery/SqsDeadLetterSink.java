package dev.concord.delivery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.store.OutboxEntry;
import java.util.Map;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Dead-lettered deliveries go to an SQS queue for a human (and can be re-driven from there). */
public final class SqsDeadLetterSink implements DeadLetterSink {

    private final SqsClient sqs;
    private final String queueUrl;
    private final ObjectMapper json;

    public SqsDeadLetterSink(SqsClient sqs, String queueUrl, ObjectMapper json) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.json = json;
    }

    @Override
    public void deadLetter(OutboxEntry entry, String reason) {
        try {
            String body = json.writeValueAsString(Map.of("event", MerchantEvent.of(entry), "reason", reason));
            sqs.sendMessage(b -> b.queueUrl(queueUrl).messageBody(body));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
