package dev.concord.engine.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.StateEngine;
import dev.concord.ingest.queue.IngestedEvent;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

/**
 * Long-polls the FIFO queue and feeds the state engine. Runs as a plain process (a container on ECS
 * Fargate in AWS, or locally).
 *
 * <p>A message is deleted only after the engine has committed it, so a crash means redelivery, which
 * the engine absorbs (per-event idempotency). If a message fails, the rest of the batch from the
 * <em>same payment</em> is left on the queue too: processing a later event of that payment first would
 * break per-payment order. Messages of other payments carry on. After {@code maxReceiveCount} failures
 * SQS moves a message to the dead-letter queue.
 */
public final class SqsStateWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SqsStateWorker.class);

    private final SqsClient sqs;
    private final String queueUrl;
    private final StateEngine engine;
    private final ObjectMapper json;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public SqsStateWorker(SqsClient sqs, String queueUrl, StateEngine engine, ObjectMapper json) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.engine = engine;
        this.json = json;
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                pollOnce(20);
            } catch (RuntimeException e) {
                log.error("poll failed; retrying", e);
                sleep(1000);
            }
        }
    }

    public void stop() {
        running.set(false);
    }

    /** One receive + process cycle. Returns how many messages were processed and deleted. */
    public int pollOnce(int waitSeconds) {
        List<Message> batch = sqs.receiveMessage(b -> b.queueUrl(queueUrl).maxNumberOfMessages(10)
                .waitTimeSeconds(waitSeconds)
                .messageSystemAttributeNames(MessageSystemAttributeName.MESSAGE_GROUP_ID)).messages();
        Set<String> blockedGroups = new HashSet<>();
        int done = 0;
        for (Message m : batch) {
            String group = m.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
            if (blockedGroups.contains(group)) {
                continue;
            }
            try {
                engine.process(json.readValue(m.body(), IngestedEvent.class));
                sqs.deleteMessage(b -> b.queueUrl(queueUrl).receiptHandle(m.receiptHandle()));
                done++;
            } catch (Exception e) {
                log.error("processing failed for group {}; holding the rest of its batch", group, e);
                blockedGroups.add(group);
            }
        }
        return done;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
