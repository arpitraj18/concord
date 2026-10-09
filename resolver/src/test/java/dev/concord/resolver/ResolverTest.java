package dev.concord.resolver;

import static dev.concord.engine.machine.CanonicalState.AUTHORIZED;
import static dev.concord.engine.machine.CanonicalState.CAPTURED;
import static dev.concord.engine.machine.CanonicalState.CREATED;
import static dev.concord.engine.machine.CanonicalState.FAILED;
import static dev.concord.engine.machine.CanonicalState.PENDING;
import static dev.concord.engine.machine.CanonicalState.SETTLED;
import static dev.concord.engine.machine.CanonicalState.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.Events;
import dev.concord.engine.StateEngine;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.InMemoryPaymentStore;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import dev.concord.resolver.SweepReport.Action;
import dev.concord.resolver.merchant.MerchantOrders;
import dev.concord.resolver.state.InMemoryResolverStateStore;
import dev.concord.resolver.status.GatewayActions;
import dev.concord.resolver.status.StatusClient;
import dev.concord.resolver.status.StatusResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class ResolverTest {

    static final ResolverConfig CONFIG = new ResolverConfig(Duration.ofMinutes(2), Duration.ofMinutes(10),
            Duration.ofHours(1), Duration.ofMinutes(30), Duration.ofMinutes(1), Duration.ofHours(24),
            Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofMinutes(5), Duration.ofHours(1), 100);

    final MutableClock clock = new MutableClock(Instant.parse("2026-10-09T10:00:00Z"));
    final InMemoryPaymentStore store = new InMemoryPaymentStore();
    final Normalizer normalizer = Normalizer.withBundledMappings(new ObjectMapper());
    final StateEngine engine = new StateEngine(normalizer, store, clock);
    final FakeGateway gateway = new FakeGateway();
    final Map<String, MerchantOrders.OrderStatus> orders = new HashMap<>();
    final InMemoryResolverStateStore resolverStates = new InMemoryResolverStateStore();
    final Resolver resolver = new Resolver(engine, normalizer, gateway, gateway,
            id -> orders.getOrDefault(id, MerchantOrders.OrderStatus.UNKNOWN), resolverStates, CONFIG, clock);

    PaymentRecord payment(String id) {
        return store.find(Gateway.A, id).orElseThrow();
    }

    /** Runs sweeps, advancing time, until nothing is due or the limit is hit. */
    void sweepFor(Duration total) {
        Instant end = clock.instant().plus(total);
        while (clock.instant().isBefore(end)) {
            resolver.sweep();
            clock.advance(Duration.ofSeconds(15));
        }
    }

    @Test
    void droppedFinalWebhookIsRecoveredFromTheStatusApi() {
        engine.process(Events.claim("p1", AUTHORIZED, "A#1", 1));
        gateway.truth.put("p1", CAPTURED);

        clock.advance(Duration.ofMinutes(1));
        assertThat(resolver.sweep().count(Action.CHECKED)).as("not old enough yet").isZero();
        clock.advance(Duration.ofMinutes(2));
        SweepReport r = resolver.sweep();
        assertThat(r.count(Action.APPLIED)).isEqualTo(1);
        assertThat(payment("p1").state()).isEqualTo(CAPTURED);
        assertThat(store.history(Gateway.A, "p1").getLast().source()).isEqualTo(dev.concord.engine.mapping.EventSource.STATUS_API);
    }

    @Test
    void registeredPaymentWithEveryWebhookDroppedStillResolves() {
        engine.register(Gateway.A, "p2", "order_2", 500L, "INR");
        gateway.truth.put("p2", SETTLED);
        sweepFor(Duration.ofMinutes(5));
        assertThat(payment("p2").state()).isEqualTo(SETTLED);
        assertThat(store.outbox(Gateway.A, "p2")).extracting(OutboxEntry::toState).containsExactly(SETTLED);
    }

    @Test
    void contradictionHeldOnTheRightSideIsVerified() {
        engine.process(Events.claim("p3", CAPTURED, "A#1", 1));
        engine.process(Events.claim("p3", FAILED, "A#2", 2));
        gateway.truth.put("p3", CAPTURED);
        resolver.sweep();
        assertThat(payment("p3").needsVerification()).isFalse();
        assertThat(payment("p3").state()).isEqualTo(CAPTURED);
    }

    @Test
    void contradictionHeldOnTheWrongSideIsCorrectedAfterASecondLook() {
        engine.process(Events.claim("p4", FAILED, "A#1", 1));     // the lie arrived first
        engine.process(Events.claim("p4", CAPTURED, "A#2", 2));   // the truth, now a "contradiction"
        gateway.truth.put("p4", CAPTURED);

        assertThat(resolver.sweep().count(Action.SUSPECTED_WRONG)).isEqualTo(1);
        assertThat(payment("p4").state()).as("one observation is not enough").isEqualTo(FAILED);
        clock.advance(Duration.ofSeconds(10));
        resolver.sweep();
        assertThat(payment("p4").state()).as("not before the confirmation delay").isEqualTo(FAILED);
        clock.advance(Duration.ofSeconds(25));
        assertThat(resolver.sweep().count(Action.CORRECTED)).isEqualTo(1);

        PaymentRecord rec = payment("p4");
        assertThat(rec.state()).isEqualTo(CAPTURED);
        assertThat(rec.needsVerification()).isFalse();
        OutboxEntry last = store.outbox(Gateway.A, "p4").getLast();
        assertThat(last.kind()).isEqualTo(OutboxEntry.Kind.CORRECTION);
        assertThat(last.fromState()).isEqualTo(FAILED);
    }

    @Test
    void lieThatFitsALegalPathIsCaughtByOneOffFinalVerification() {
        engine.process(Events.claim("p5", AUTHORIZED, "A#1", 1));
        engine.process(Events.claim("p5", CAPTURED, "A#2", 2));   // fake: the gateway never captured
        gateway.truth.put("p5", AUTHORIZED);
        resolver.sweep();
        assertThat(payment("p5").state()).as("not before verifyFinalAfter").isEqualTo(CAPTURED);

        sweepFor(Duration.ofMinutes(2));
        assertThat(payment("p5").state()).isEqualTo(AUTHORIZED);
        assertThat(store.outbox(Gateway.A, "p5").getLast().kind()).isEqualTo(OutboxEntry.Kind.CORRECTION);
    }

    @Test
    void truthfulFinalStateIsVerifiedOnceNotPolledForever() {
        engine.process(Events.claim("p9", FAILED, "A#1", 1));
        gateway.truth.put("p9", FAILED);
        sweepFor(Duration.ofMinutes(10));
        assertThat(gateway.calls).isEqualTo(1);
    }

    @Test
    void unreachableStatusApiBacksOffMarksUnknownAndFinallyParks() {
        engine.process(Events.claim("p6", PENDING, "A#1", 1));
        gateway.down = true;
        sweepFor(Duration.ofMinutes(15));
        assertThat(payment("p6").state()).as("pending + unreachable for 10 min").isEqualTo(UNKNOWN);
        assertThat(store.outbox(Gateway.A, "p6").getLast().toState())
                .as("the merchant is told 'uncertain, do not cancel'").isEqualTo(UNKNOWN);

        int checksSoFar = gateway.calls;
        sweepFor(Duration.ofMinutes(2));
        assertThat(gateway.calls - checksSoFar).as("backoff: far fewer calls than sweeps").isLessThan(8);

        sweepFor(Duration.ofMinutes(30));
        assertThat(resolverStates.parked()).as("not before the time budget").isEmpty();
        sweepFor(Duration.ofMinutes(30));
        assertThat(resolverStates.parked()).extracting(s -> s.paymentKey()).contains("A#p6");
    }

    @Test
    void configRefusesToParkBeforeUnknownOrCapture() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new ResolverConfig(Duration.ofMinutes(2),
                        Duration.ofMinutes(10), Duration.ofHours(1), Duration.ofMinutes(30), Duration.ofMinutes(1),
                        Duration.ofHours(24), Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofMinutes(5),
                        Duration.ofMinutes(20), 100))
                .hasMessageContaining("parkAfter");
    }

    @Test
    void unknownResolvesOnceTheStatusApiIsBack() {
        engine.process(Events.claim("p7", PENDING, "A#1", 1));
        gateway.down = true;
        sweepFor(Duration.ofMinutes(13));
        assertThat(payment("p7").state()).isEqualTo(UNKNOWN);
        gateway.down = false;
        gateway.truth.put("p7", CAPTURED);
        sweepFor(Duration.ofMinutes(10));
        assertThat(payment("p7").state()).isEqualTo(CAPTURED);
    }

    @Test
    void longAuthorizedPaymentIsCapturedIfTheOrderIsValidAndReleasedIfNot() {
        engine.register(Gateway.A, "keep", "order_keep", 100L, "INR");
        engine.register(Gateway.A, "drop", "order_drop", 100L, "INR");
        engine.process(Events.claim("keep", AUTHORIZED, "A#k", 1));
        engine.process(Events.claim("drop", AUTHORIZED, "A#d", 2));
        gateway.truth.put("keep", AUTHORIZED);
        gateway.truth.put("drop", AUTHORIZED);
        orders.put("order_keep", MerchantOrders.OrderStatus.VALID);
        orders.put("order_drop", MerchantOrders.OrderStatus.CANCELLED);

        sweepFor(Duration.ofMinutes(20));
        assertThat(gateway.actions).isEmpty();
        sweepFor(Duration.ofMinutes(20));
        assertThat(gateway.actions).containsExactlyInAnyOrder("capture keep", "release drop");
        assertThat(payment("keep").state()).isEqualTo(CAPTURED);
        assertThat(payment("drop").state()).isEqualTo(FAILED);
        sweepFor(Duration.ofMinutes(20));
        assertThat(gateway.actions).as("never twice").hasSize(2);
    }

    @Test
    void webhookAndPollRacingForOnePaymentAgree() throws Exception {
        for (int i = 0; i < 50; i++) {
            String id = "race" + i;
            String webhookKey = "A#c" + (1000 + i);
            engine.process(Events.claim(id, AUTHORIZED, "A#a" + i, 1));
            gateway.truth.put(id, CAPTURED);
            clock.advance(Duration.ofMinutes(3));
            PaymentRecord snapshot = payment(id);
            CountDownLatch go = new CountDownLatch(1);
            try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
                var w = pool.submit(() -> { go.await(); return engine.process(Events.claim(id, CAPTURED, webhookKey, 2)); });
                var p = pool.submit(() -> { go.await(); resolver.resolve(snapshot, clock.instant(), new SweepReport()); return null; });
                go.countDown();
                w.get();
                p.get();
            }
            PaymentRecord rec = payment(id);
            assertThat(rec.state()).isEqualTo(CAPTURED);
            assertThat(store.outbox(Gateway.A, id)).as("exactly one CAPTURED transition").extracting(OutboxEntry::toState)
                    .containsExactly(AUTHORIZED, CAPTURED);
        }
    }

    @Test
    void correctionIsAbandonedIfThePaymentChangedBetweenTheTwoLooks() {
        engine.process(Events.claim("p8", AUTHORIZED, "A#1", 1));
        engine.process(Events.claim("p8", CAPTURED, "A#2", 2));
        engine.process(Events.claim("p8", FAILED, "A#3", 3));     // flag it
        gateway.truth.put("p8", AUTHORIZED);                      // status API lags
        resolver.sweep();
        engine.process(Events.claim("p8", SETTLED, "A#4", 4));    // real progress in between
        gateway.truth.put("p8", SETTLED);
        clock.advance(Duration.ofSeconds(40));
        resolver.sweep();
        assertThat(payment("p8").state()).isEqualTo(SETTLED);
        assertThat(store.outbox(Gateway.A, "p8")).extracting(OutboxEntry::kind).doesNotContain(OutboxEntry.Kind.CORRECTION);
    }

    static final class FakeGateway implements StatusClient, GatewayActions {
        final Map<String, CanonicalState> truth = new HashMap<>();
        final List<String> actions = new ArrayList<>();
        volatile boolean down;
        volatile int calls;

        @Override
        public synchronized StatusResult fetch(Gateway g, String paymentId) {
            calls++;
            if (down) {
                return new StatusResult.Unavailable("HTTP 503");
            }
            CanonicalState s = truth.get(paymentId);
            return s == null ? new StatusResult.NotFound() : new StatusResult.Found(s, s.name().toLowerCase());
        }

        @Override
        public synchronized CanonicalState capture(Gateway g, String paymentId) {
            actions.add("capture " + paymentId);
            truth.put(paymentId, CAPTURED);
            return CAPTURED;
        }

        @Override
        public synchronized CanonicalState release(Gateway g, String paymentId) {
            actions.add("release " + paymentId);
            truth.put(paymentId, FAILED);
            return FAILED;
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
