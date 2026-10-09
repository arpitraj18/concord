package dev.concord.engine.machine;

import static org.assertj.core.api.Assertions.assertThat;

import dev.concord.engine.Events;
import dev.concord.engine.StateEngine;
import dev.concord.engine.mapping.CanonicalEvent;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.EventOutcome;
import dev.concord.engine.store.HistoryEntry;
import dev.concord.engine.store.InMemoryPaymentStore;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Phase 2 "done when", part 1: thousands of random event sequences never produce an illegal state.
 * Sequences mix every state (UNKNOWN included), repeats, unmapped events, SQS redeliveries and
 * contradictions, in any order.
 */
class StateEnginePropertiesTest {

    private static final Normalizer NORMALIZER = Normalizer.withBundledMappings(new ObjectMapper());

    record Run(PaymentRecord record, List<HistoryEntry> history, List<OutboxEntry> outbox) {
    }

    static Run run(List<CanonicalEvent> events) {
        InMemoryPaymentStore store = new InMemoryPaymentStore();
        StateEngine engine = new StateEngine(NORMALIZER, store, Clock.systemUTC());
        events.forEach(engine::process);
        return new Run(store.find(Gateway.A, "pay_p").orElse(PaymentRecord.unseen(Gateway.A, "pay_p")),
                store.history(Gateway.A, "pay_p"), store.outbox(Gateway.A, "pay_p"));
    }

    @Property(tries = 3000)
    void stateOnlyEverMovesAlongLegalSteps(@ForAll("sequences") List<CanonicalEvent> events) {
        Run r = run(events);
        CanonicalState previous = CanonicalState.CREATED;
        long previousVersion = 0;
        // Outbox entries are the applied transitions, in the order they were committed.
        for (OutboxEntry o : r.outbox()) {
            assertThat(o.fromState()).isEqualTo(previous);
            assertThat(previous.precedes(o.toState())).as("%s -> %s", previous, o.toState()).isTrue();
            assertThat(o.version()).isGreaterThan(previousVersion);
            previous = o.toState();
            previousVersion = o.version();
        }
        assertThat(r.record().state()).isEqualTo(previous);
        for (HistoryEntry h : r.history()) {
            for (int i = 1; i < h.path().size(); i++) {
                assertThat(h.path().get(i - 1).canMoveTo(h.path().get(i))).as("step in %s", h.path()).isTrue();
            }
        }
    }

    @Property(tries = 2000)
    void consistentEvidenceGivesTheSameResultInAnyOrder(@ForAll("sequences") List<CanonicalEvent> events, @ForAll long seed) {
        List<CanonicalEvent> shuffled = new ArrayList<>(events);
        Collections.shuffle(shuffled, new Random(seed));
        Run a = run(events);
        Run b = run(shuffled);

        assertThat(b.record().needsVerification()).as("the flag never depends on order")
                .isEqualTo(a.record().needsVerification());
        if (!a.record().needsVerification()) {
            assertThat(b.record().state()).isEqualTo(a.record().state());
            assertThat(a.record().state()).isEqualTo(furthest(events));
        }
    }

    @Property(tries = 2000)
    void flaggedExactlyWhenTheGatewayContradictedItself(@ForAll("sequences") List<CanonicalEvent> events) {
        List<CanonicalState> claims = events.stream().map(CanonicalEvent::claimedState).filter(s -> s != null).toList();
        boolean contradictory = false;
        for (CanonicalState x : claims) {
            for (CanonicalState y : claims) {
                contradictory |= x != y && !x.precedes(y) && !y.precedes(x);
            }
        }
        assertThat(run(events).record().needsVerification()).isEqualTo(contradictory);
    }

    @Property(tries = 1000)
    void everyDistinctEventIsRecordedOnceAndRedeliveriesChangeNothing(@ForAll("sequences") List<CanonicalEvent> events) {
        Run r = run(events);
        long distinct = events.stream().map(CanonicalEvent::dedupKey).distinct().count();
        assertThat(r.history()).hasSize((int) distinct);
        long changes = r.history().stream()
                .filter(h -> h.outcome() == EventOutcome.APPLIED || h.outcome() == EventOutcome.CONTRADICTION).count();
        assertThat(r.record().version()).isEqualTo(changes);
        assertThat(r.outbox()).extracting(OutboxEntry::idempotencyKey).doesNotHaveDuplicates();
    }

    static CanonicalState furthest(List<CanonicalEvent> events) {
        CanonicalState max = CanonicalState.CREATED;
        for (CanonicalEvent e : events) {
            if (e.claimedState() != null && max.precedes(e.claimedState())) {
                max = e.claimedState();
            }
        }
        return max;
    }

    @Provide
    Arbitrary<List<CanonicalEvent>> sequences() {
        Arbitrary<CanonicalState> claim = Arbitraries.frequencyOf(
                net.jqwik.api.Tuple.of(3, Arbitraries.of(CanonicalState.PENDING, CanonicalState.AUTHORIZED,
                        CanonicalState.CAPTURED, CanonicalState.SETTLED, CanonicalState.REFUNDED)),
                net.jqwik.api.Tuple.of(1, Arbitraries.of(CanonicalState.FAILED, CanonicalState.UNKNOWN)),
                net.jqwik.api.Tuple.of(1, Arbitraries.just(null)));
        // Small key space so the same event is often delivered more than once (an SQS redelivery).
        Arbitrary<Integer> keyIndex = Arbitraries.integers().between(0, 14);
        return net.jqwik.api.Combinators.combine(keyIndex, claim).as((k, s) -> new int[] {k, s == null ? -1 : s.ordinal()})
                .list().ofMinSize(0).ofMaxSize(25)
                .map(items -> {
                    // The same dedup key always means the same event, so its claim is fixed per key.
                    CanonicalState[] byKey = new CanonicalState[15];
                    boolean[] set = new boolean[15];
                    List<CanonicalEvent> out = new ArrayList<>();
                    long t = 0;
                    for (int[] item : items) {
                        int k = item[0];
                        if (!set[k]) {
                            byKey[k] = item[1] < 0 ? null : CanonicalState.values()[item[1]];
                            set[k] = true;
                        }
                        out.add(Events.claim("pay_p", byKey[k], "A#evt_" + k, t++));
                    }
                    return out;
                });
    }
}
