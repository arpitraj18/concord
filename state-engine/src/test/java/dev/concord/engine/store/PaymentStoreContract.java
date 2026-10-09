package dev.concord.engine.store;

import static dev.concord.engine.machine.CanonicalState.AUTHORIZED;
import static dev.concord.engine.machine.CanonicalState.CAPTURED;
import static dev.concord.engine.machine.CanonicalState.FAILED;
import static dev.concord.engine.machine.CanonicalState.PENDING;
import static dev.concord.engine.machine.CanonicalState.SETTLED;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.Events;
import dev.concord.engine.ProcessResult;
import dev.concord.engine.StateEngine;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.CanonicalEvent;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.ingest.Gateway;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/** Rules every PaymentStore must follow; run against the in-memory store and against DynamoDB Local. */
public abstract class PaymentStoreContract {

    protected abstract PaymentStore store();

    private StateEngine engine() {
        return new StateEngine(Normalizer.withBundledMappings(new ObjectMapper()), store(), Clock.systemUTC());
    }

    private String pid(String name) {
        return name + "_" + System.nanoTime();
    }

    @Test
    void appliesRecordsHistoryAndWritesOutboxTogether() {
        String id = pid("pay");
        StateEngine engine = engine();
        engine.process(Events.claim(id, AUTHORIZED, "A#e1", 1));
        ProcessResult r = engine.process(Events.claim(id, CAPTURED, "A#e2", 2));

        PaymentRecord rec = store().find(Gateway.A, id).orElseThrow();
        assertThat(rec.state()).isEqualTo(CAPTURED);
        assertThat(rec.version()).isEqualTo(2);
        assertThat(rec.amountMinor()).isEqualTo(49_900L);
        assertThat(r.outcome()).isEqualTo(EventOutcome.APPLIED);
        assertThat(store().history(Gateway.A, id)).extracting(HistoryEntry::dedupKey).containsExactly("A#e1", "A#e2");
        assertThat(store().history(Gateway.A, id).getFirst().path()).containsExactly(CanonicalState.CREATED, PENDING, AUTHORIZED);
        assertThat(store().outbox(Gateway.A, id)).extracting(OutboxEntry::toState).containsExactly(AUTHORIZED, CAPTURED);
        assertThat(store().outbox(Gateway.A, id).getLast().amountMinor()).isEqualTo(49_900L);
    }

    @Test
    void redeliveredEventIsRecognisedAndChangesNothing() {
        String id = pid("redeliver");
        StateEngine engine = engine();
        engine.process(Events.claim(id, CAPTURED, "A#e1", 1));
        ProcessResult again = engine.process(Events.claim(id, CAPTURED, "A#e1", 1));
        assertThat(again.redelivery()).isTrue();
        assertThat(store().find(Gateway.A, id).orElseThrow().version()).isEqualTo(1);
        assertThat(store().history(Gateway.A, id)).hasSize(1);
    }

    @Test
    void staleAndSameEventsAreRecordedWithoutChangingState() {
        String id = pid("stale");
        StateEngine engine = engine();
        engine.process(Events.claim(id, CAPTURED, "A#e1", 1));
        assertThat(engine.process(Events.claim(id, AUTHORIZED, "A#e0", 2)).outcome()).isEqualTo(EventOutcome.STALE);
        assertThat(engine.process(Events.claim(id, CAPTURED, "A#e9", 3)).outcome()).isEqualTo(EventOutcome.SAME);
        assertThat(engine.process(Events.claim(id, null, "A#e8", 4)).outcome()).isEqualTo(EventOutcome.UNMAPPED);
        assertThat(store().find(Gateway.A, id).orElseThrow().version()).isEqualTo(1);
        assertThat(store().history(Gateway.A, id)).hasSize(4);
    }

    @Test
    void contradictionFlagsWithoutMoving() {
        String id = pid("lie");
        StateEngine engine = engine();
        engine.process(Events.claim(id, CAPTURED, "A#e1", 1));
        engine.process(Events.claim(id, FAILED, "A#e2", 2));
        PaymentRecord rec = store().find(Gateway.A, id).orElseThrow();
        assertThat(rec.state()).isEqualTo(CAPTURED);
        assertThat(rec.needsVerification()).isTrue();
        assertThat(rec.verificationReason()).contains("FAILED");
        assertThat(store().outbox(Gateway.A, id)).hasSize(1);
    }

    @Test
    void unmappedFirstEventLeavesNoStateRecord() {
        String id = pid("unmapped");
        engine().process(Events.claim(id, null, "A#e1", 1));
        assertThat(store().find(Gateway.A, id)).isEmpty();
        assertThat(store().history(Gateway.A, id)).hasSize(1);
    }

    @Test
    void stuckPaymentsCanBeFoundByStateAndAge() {
        String id = pid("stuck");
        engine().process(Events.claim(id, PENDING, "A#e1", 1));
        Instant later = Instant.now().plusSeconds(60);
        assertThat(store().findInStateSince(PENDING, later, 1000)).extracting(PaymentRecord::paymentId).contains(id);
        assertThat(store().findInStateSince(PENDING, Instant.EPOCH, 1000)).isEmpty();
    }

    @Test
    void timeWindowQueriesRespectSubSecondOrder() {
        // Instant.toString() drops trailing zeros: "…:00Z" would sort after "…:00.5Z" as text.
        java.time.Instant second = java.time.Instant.parse("2030-01-01T10:00:00Z");
        String a = pid("win_whole");
        String b = pid("win_half");
        new StateEngine(Normalizer.withBundledMappings(new ObjectMapper()), store(), java.time.Clock.fixed(second, java.time.ZoneOffset.UTC))
                .process(Events.claim(a, SETTLED, "A#w1", 1));
        new StateEngine(Normalizer.withBundledMappings(new ObjectMapper()), store(),
                java.time.Clock.fixed(second.plusMillis(500), java.time.ZoneOffset.UTC)).process(Events.claim(b, SETTLED, "A#w2", 2));

        assertThat(store().findInStateBetween(SETTLED, second, second.plusMillis(250), 1000))
                .extracting(PaymentRecord::paymentId).containsExactly(a);
        assertThat(store().findInStateBetween(SETTLED, second.plusMillis(250), second.plusSeconds(1), 1000))
                .extracting(PaymentRecord::paymentId).containsExactly(b);
        assertThat(store().findInStateBetween(SETTLED, second, second, 1000)).isEmpty();
    }

    @Test
    void registrationMakesAPaymentKnownBeforeAnyWebhook() {
        String id = pid("reg");
        StateEngine engine = engine();
        assertThat(engine.register(Gateway.A, id, "order_9", 12_300L, "INR").outcome()).isEqualTo(EventOutcome.REGISTERED);
        assertThat(engine.register(Gateway.A, id, "order_9", 12_300L, "INR").outcome()).as("idempotent").isEqualTo(EventOutcome.SAME);
        PaymentRecord rec = store().find(Gateway.A, id).orElseThrow();
        assertThat(rec.state()).isEqualTo(CanonicalState.CREATED);
        assertThat(rec.orderId()).isEqualTo("order_9");
        assertThat(store().findInStateSince(CanonicalState.CREATED, Instant.now().plusSeconds(5), 1000))
                .extracting(PaymentRecord::paymentId).contains(id);
        engine.process(Events.claim(id, CAPTURED, "A#c1", 1));
        assertThat(store().find(Gateway.A, id).orElseThrow().state()).isEqualTo(CAPTURED);
    }

    @Test
    void flaggedPaymentsAreFoundAndVerificationClearsTheFlag() {
        String id = pid("verify");
        StateEngine engine = engine();
        engine.process(Events.claim(id, CAPTURED, "A#v1", 1));
        engine.process(Events.claim(id, FAILED, "A#v2", 2));
        assertThat(store().findFlagged(1000)).extracting(PaymentRecord::paymentId).contains(id);

        assertThat(engine.verify(Gateway.A, id, CAPTURED, "status api: captured").outcome()).isEqualTo(EventOutcome.VERIFIED);
        PaymentRecord rec = store().find(Gateway.A, id).orElseThrow();
        assertThat(rec.needsVerification()).isFalse();
        assertThat(store().findFlagged(1000)).extracting(PaymentRecord::paymentId).doesNotContain(id);
    }

    @Test
    void correctionMovesToTheTruthAuditsItAndTellsTheMerchant() {
        String id = pid("correct");
        StateEngine engine = engine();
        engine.process(Events.claim(id, AUTHORIZED, "A#k1", 1));
        engine.process(Events.claim(id, CAPTURED, "A#k2", 2)); // a lie that fits a legal path
        long version = store().find(Gateway.A, id).orElseThrow().version();

        assertThat(engine.correct(Gateway.A, id, AUTHORIZED, version - 1, "stale decision").outcome())
                .as("refused when the payment changed since the resolver looked").isNull();
        assertThat(engine.correct(Gateway.A, id, AUTHORIZED, version, "status api: authorized x2").outcome())
                .isEqualTo(EventOutcome.CORRECTED);

        PaymentRecord rec = store().find(Gateway.A, id).orElseThrow();
        assertThat(rec.state()).isEqualTo(AUTHORIZED);
        OutboxEntry last = store().outbox(Gateway.A, id).getLast();
        assertThat(last.kind()).isEqualTo(OutboxEntry.Kind.CORRECTION);
        assertThat(last.fromState()).isEqualTo(CAPTURED);
        assertThat(last.toState()).isEqualTo(AUTHORIZED);
        assertThat(last.orderId()).isEqualTo("order_1");
        assertThat(store().history(Gateway.A, id)).extracting(HistoryEntry::outcome).contains(EventOutcome.CORRECTED);
    }

    @Test
    void concurrentWritersOnOnePaymentLoseNoUpdates() throws Exception {
        String id = pid("race");
        List<CanonicalEvent> events = List.of(
                Events.claim(id, PENDING, "A#r1", 1), Events.claim(id, AUTHORIZED, "A#r2", 2),
                Events.claim(id, CAPTURED, "A#r3", 3), Events.claim(id, SETTLED, "A#r4", 4),
                Events.claim(id, CAPTURED, "A#r5", 5), Events.claim(id, AUTHORIZED, "A#r6", 6),
                Events.claim(id, SETTLED, "A#r4", 4), Events.claim(id, PENDING, "A#r1", 1));
        StateEngine engine = engine();
        CountDownLatch go = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(events.size())) {
            List<Future<ProcessResult>> futures = new ArrayList<>();
            for (CanonicalEvent e : events) {
                futures.add(pool.submit(() -> {
                    go.await();
                    return engine.process(e);
                }));
            }
            go.countDown();
            for (Future<ProcessResult> f : futures) {
                f.get();
            }
        }
        PaymentRecord rec = store().find(Gateway.A, id).orElseThrow();
        assertThat(rec.state()).isEqualTo(SETTLED);
        assertThat(store().history(Gateway.A, id)).hasSize(6);
        List<OutboxEntry> outbox = store().outbox(Gateway.A, id);
        assertThat(rec.version()).isEqualTo(outbox.size());
        CanonicalState previous = CanonicalState.CREATED;
        for (OutboxEntry o : outbox) {
            assertThat(o.fromState()).isEqualTo(previous);
            previous = o.toState();
        }
    }
}
