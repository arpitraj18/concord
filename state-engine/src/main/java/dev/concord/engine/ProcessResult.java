package dev.concord.engine;

import dev.concord.engine.store.EventOutcome;
import dev.concord.engine.store.PaymentRecord;

/**
 * @param outcome null when the event had already been processed (a redelivery); nothing was written
 * @param record  the payment after processing (unchanged for a redelivery)
 */
public record ProcessResult(EventOutcome outcome, PaymentRecord record, int attempts) {

    public boolean redelivery() {
        return outcome == null;
    }
}
