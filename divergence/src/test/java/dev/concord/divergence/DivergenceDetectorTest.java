package dev.concord.divergence;

import static dev.concord.engine.machine.CanonicalState.AUTHORIZED;
import static dev.concord.engine.machine.CanonicalState.CAPTURED;
import static dev.concord.engine.machine.CanonicalState.REFUNDED;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.Events;
import dev.concord.engine.StateEngine;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.InMemoryPaymentStore;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import dev.concord.resolver.merchant.MerchantOrder;
import dev.concord.resolver.merchant.MerchantOrders;
import dev.concord.resolver.status.StatusClient;
import dev.concord.resolver.status.StatusResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class DivergenceDetectorTest {

    static final DetectorConfig CONFIG = new DetectorConfig(Duration.ofMinutes(2), Duration.ofSeconds(30),
            Duration.ofDays(1), Duration.ofMinutes(15), 50);

    final MutableClock clock = new MutableClock(Instant.parse("2026-10-11T10:00:00Z"));
    final InMemoryPaymentStore store = new InMemoryPaymentStore();
    final StateEngine engine = new StateEngine(Normalizer.withBundledMappings(new ObjectMapper()), store, clock);
    final Map<String, CanonicalState> gateway = new ConcurrentHashMap<>();
    final FakeMerchant merchant = new FakeMerchant();
    final InMemoryIncidentStore incidents = new InMemoryIncidentStore();
    final DetectorStateStore.InMemory states = new DetectorStateStore.InMemory();
    int statusCalls;
    final StatusClient status = (g, id) -> {
        statusCalls++;
        CanonicalState s = gateway.get(id);
        return s == null ? new StatusResult.NotFound() : new StatusResult.Found(s, s.name());
    };
    final DivergenceDetector detector = new DivergenceDetector(engine, status, merchant, incidents, states, CONFIG, clock);

    /** A registered payment, an order at the merchant, and the gateway's truth. */
    void payment(String id, long amount, CanonicalState concordState, String orderStatus, CanonicalState truth) {
        engine.register(Gateway.A, id, "o_" + id, amount, "INR");
        if (concordState != CanonicalState.CREATED) {
            engine.process(Events.claim(id, concordState, "A#" + id + concordState, 1));
        }
        merchant.set("o_" + id, id, orderStatus, amount);
        gateway.put(id, truth);
    }

    PaymentRecord rec(String id) {
        return store.find(Gateway.A, id).orElseThrow();
    }

    @Test
    void missedWebhookAndCheckoutTimeoutIsRaisedAsPaidButCancelledAfterTheGracePeriod() {
        payment("late", 49_900, CAPTURED, "CANCELLED", CAPTURED);

        DivergenceDetector.CheckResult first = detector.check(rec("late"));
        assertThat(first.found()).containsExactly(DivergenceType.PAID_BUT_CANCELLED);
        assertThat(first.raised()).as("not before the grace period").isEmpty();

        clock.advance(Duration.ofMinutes(2));
        assertThat(detector.runDue()).as("a re-check was scheduled for the end of the grace period").isEqualTo(1);
        Incident inc = incidents.open(10).getFirst();
        assertThat(inc.type()).isEqualTo(DivergenceType.PAID_BUT_CANCELLED);
        assertThat(inc.firstSeenAt()).isEqualTo(Instant.parse("2026-10-11T10:00:00Z"));
        assertThat(inc.severity()).isEqualTo(Severity.MEDIUM);
        assertThat(inc.explanation()).contains("charged 499.00 INR").contains("CANCELLED").contains("Refund");
    }

    @Test
    void eventStillBeingDeliveredIsNotAnIncident() {
        payment("inflight", 49_900, CAPTURED, "AWAITING_PAYMENT", CAPTURED);
        assertThat(detector.check(rec("inflight")).found()).containsExactly(DivergenceType.PAID_BUT_AWAITING);
        merchant.set("o_inflight", "inflight", "PAID", 49_900);  // the delivery lands
        clock.advance(Duration.ofMinutes(3));
        detector.runDue();
        assertThat(incidents.all()).isEmpty();
    }

    @Test
    void incidentResolvesItselfWhenTheDivergenceGoesAwayAndSeverityGrowsWhileItLasts() {
        payment("big", 2_000_000, CAPTURED, "CANCELLED", CAPTURED);
        detector.check(rec("big"));
        clock.advance(Duration.ofMinutes(2));
        detector.check(rec("big"));
        long scoreAtRaise = incidents.open(1).getFirst().score();

        clock.advance(Duration.ofHours(1));
        detector.check(rec("big"));
        assertThat(incidents.open(1).getFirst().score()).isGreaterThan(scoreAtRaise);
        assertThat(incidents.open(1).getFirst().severity()).isEqualTo(Severity.CRITICAL);

        merchant.set("o_big", "big", "PAID", 2_000_000); // someone reinstated the order
        DivergenceDetector.CheckResult r = detector.check(rec("big"));
        assertThat(r.resolved()).hasSize(1);
        assertThat(incidents.open(10)).isEmpty();
        assertThat(incidents.all().getFirst().resolution()).contains("no longer present");
    }

    @Test
    void redirectBugIsUnpaidButFulfilled() {
        payment("redirect", 100_000, CanonicalState.FAILED, "PAID", CanonicalState.FAILED);
        detector.check(rec("redirect"));
        clock.advance(Duration.ofMinutes(2));
        detector.check(rec("redirect"));
        assertThat(incidents.open(10)).extracting(Incident::type).containsExactly(DivergenceType.UNPAID_BUT_FULFILLED);
    }

    @Test
    void driftBetweenConcordAndGatewayIsHandedToTheResolver() {
        payment("drift", 49_900, CAPTURED, "PAID", REFUNDED); // refund webhook lost, Concord never re-checked
        detector.check(rec("drift"));
        clock.advance(Duration.ofMinutes(2));
        detector.check(rec("drift"));
        assertThat(incidents.open(10)).extracting(Incident::type)
                .containsExactlyInAnyOrder(DivergenceType.CONCORD_DRIFT, DivergenceType.REFUNDED_BUT_ACTIVE);
        assertThat(rec("drift").needsVerification()).as("flagged for the resolver").isTrue();
        assertThat(store.findFlagged(10)).extracting(PaymentRecord::paymentId).contains("drift");
    }

    @Test
    void outboxChangesTriggerAChecksAfterTheSettleDelay() {
        payment("ob", 49_900, AUTHORIZED, "AWAITING_PAYMENT", AUTHORIZED);
        detector.onOutbox(store.outbox(Gateway.A, "ob").getLast());
        assertThat(detector.runDue()).isZero();
        clock.advance(Duration.ofSeconds(30));
        assertThat(detector.runDue()).isEqualTo(1);
    }

    @Test
    void merchantChangeFeedTriggersChecksAndRemembersItsCursor() {
        payment("f1", 100, AUTHORIZED, "AWAITING_PAYMENT", AUTHORIZED);
        payment("f2", 100, AUTHORIZED, "AWAITING_PAYMENT", AUTHORIZED);
        assertThat(detector.pollMerchantChanges()).isEqualTo(2);
        assertThat(detector.pollMerchantChanges()).as("cursor saved: nothing new").isZero();
        merchant.set("o_f1", "f1", "CANCELLED", 100);
        assertThat(detector.pollMerchantChanges()).isEqualTo(1);
        assertThat(detector.runDue()).isEqualTo(2);
    }

    @Test
    void sweepSkipsPaymentsCheckedRecently() {
        for (int i = 0; i < 20; i++) {
            payment("s" + i, 100, CAPTURED, "PAID", CAPTURED);
        }
        assertThat(detector.sweep()).as("the window [now - window, now) excludes this very instant").isZero();
        clock.advance(Duration.ofSeconds(1));
        assertThat(detector.sweep()).isEqualTo(20);
        clock.advance(Duration.ofMinutes(5));
        assertThat(detector.sweep()).as("all checked 5 min ago").isZero();
        clock.advance(Duration.ofMinutes(11));
        assertThat(detector.sweep()).isEqualTo(20);
        assertThat(incidents.all()).as("clean payments: no false positives").isEmpty();
    }

    /** In-memory merchant with a change feed ordered by a logical clock. */
    static final class FakeMerchant implements MerchantOrders {
        final Map<String, MerchantOrder> orders = new ConcurrentHashMap<>();
        final List<String> changes = new ArrayList<>();

        synchronized void set(String orderId, String paymentId, String status, long amount) {
            orders.put(orderId, new MerchantOrder(orderId, "A", paymentId, status, amount, "INR", null, null));
            changes.add(orderId);
        }

        @Override
        public Optional<MerchantOrder> getOrder(String orderId) {
            return Optional.ofNullable(orders.get(orderId));
        }

        @Override
        public synchronized Page changedSince(Cursor cursor, int limit) {
            int from = cursor.after().isEmpty() ? 0 : Integer.parseInt(cursor.after());
            List<MerchantOrder> page = changes.subList(from, Math.min(changes.size(), from + limit)).stream().map(orders::get).toList();
            return new Page(page, new Cursor("x", Integer.toString(from + page.size())));
        }
    }

    static final class MutableClock extends Clock {
        private volatile Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
