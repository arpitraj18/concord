package dev.concord.delivery;

import dev.concord.engine.store.OutboxEntry;

/**
 * Where deliveries go after the last retry: a human looks at them (an SQS dead-letter queue in AWS).
 * Nothing is lost: the outbox item stays in DynamoDB and can be redelivered.
 */
public interface DeadLetterSink {

    void deadLetter(OutboxEntry entry, String reason);
}
