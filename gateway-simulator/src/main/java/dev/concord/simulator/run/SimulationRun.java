package dev.concord.simulator.run;

import dev.concord.simulator.model.Scenario;
import dev.concord.simulator.model.SimState;
import dev.concord.simulator.model.Step;
import dev.concord.simulator.plan.DeliveryPlan;
import dev.concord.simulator.plan.PaymentTruth;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/** A plan being executed against the clock. */
public final class SimulationRun {

    private final String id;
    private final Scenario scenario;
    private final DeliveryPlan plan;
    private final Instant createdAt;
    private final ConcurrentLinkedQueue<DeliveryAttempt> attempts = new ConcurrentLinkedQueue<>();
    private final List<Future<?>> scheduled = new ArrayList<>();
    private final AtomicInteger outstanding = new AtomicInteger();
    private final SplittableRandom statusApiRng;
    /** Timelines changed by merchant actions (capture/void); they replace the planned future. */
    private final Map<String, PaymentTruth> actedOn = new ConcurrentHashMap<>();
    private final Map<String, Long> actedAt = new ConcurrentHashMap<>();

    private volatile RunStatus status = RunStatus.PLANNED;
    private volatile Instant startedAt;
    private volatile Instant finishedAt;

    SimulationRun(String id, Scenario scenario, DeliveryPlan plan, Instant createdAt) {
        this.id = id;
        this.scenario = scenario;
        this.plan = plan;
        this.createdAt = createdAt;
        this.statusApiRng = new SplittableRandom(plan.seed() ^ 0x5DEECE66DL);
    }

    public String id() {
        return id;
    }

    public Scenario scenario() {
        return scenario;
    }

    public DeliveryPlan plan() {
        return plan;
    }

    public double timeScale() {
        return scenario.timeScale();
    }

    public String targetUrl() {
        return scenario.targetUrl();
    }

    /** The true timeline of a payment: the plan, unless a merchant action rewrote its future. */
    public PaymentTruth truth(String paymentId) {
        PaymentTruth acted = actedOn.get(paymentId);
        return acted != null ? acted : plan.payment(paymentId).orElse(null);
    }

    /** Virtual time of the merchant action on this payment, or null if none. */
    public Long actedAt(String paymentId) {
        return actedAt.get(paymentId);
    }

    /**
     * A merchant captures or voids an authorized payment. Allowed only while the payment really is
     * AUTHORIZED; the planned future of the payment is discarded and replaced by {@code target}.
     *
     * @throws IllegalStateException with the current state if the payment is not AUTHORIZED
     */
    public synchronized PaymentTruth act(String paymentId, SimState target, long virtualNow) {
        PaymentTruth base = truth(paymentId);
        SimState current = base.stateAt(virtualNow);
        if (current != SimState.AUTHORIZED) {
            throw new IllegalStateException(current.name());
        }
        List<Step> steps = new ArrayList<>(base.steps().stream().filter(st -> st.atMs() <= virtualNow).toList());
        steps.add(new Step(target, virtualNow));
        PaymentTruth acted = new PaymentTruth(base.paymentId(), base.orderId(), base.amount(), base.currency(), steps);
        actedOn.put(paymentId, acted);
        actedAt.put(paymentId, virtualNow);
        return acted;
    }

    /** One more webhook to deliver (from a merchant action); reopens a completed run. */
    synchronized void addDelivery() {
        outstanding.incrementAndGet();
        if (status == RunStatus.COMPLETED) {
            status = RunStatus.RUNNING;
            finishedAt = null;
        }
    }

    public RunStatus status() {
        return status;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant startedAt() {
        return startedAt;
    }

    public Instant finishedAt() {
        return finishedAt;
    }

    public List<DeliveryAttempt> attempts() {
        return List.copyOf(attempts);
    }

    public int outstanding() {
        return outstanding.get();
    }

    /** Virtual ms elapsed since start, or -1 before start. */
    public long virtualElapsedMs(Instant now) {
        Instant started = startedAt;
        if (started == null) {
            return -1;
        }
        return (long) (Duration.between(started, now).toMillis() * timeScale());
    }

    /** Real ms to wait for a virtual duration. */
    public long realDelayMs(long virtualMs) {
        return (long) Math.ceil(virtualMs / timeScale());
    }

    /** Wall-clock instant at which a virtual time happens(ed). */
    public Instant realTimeOf(long virtualMs) {
        return startedAt.plusMillis(realDelayMs(virtualMs));
    }

    /** Decides whether one status API call fails at random. Synchronised because status calls are concurrent. */
    public synchronized boolean statusApiRandomFailure(double rate) {
        return rate > 0 && statusApiRng.nextDouble() < rate;
    }

    synchronized boolean markStarted(Instant now, int toDeliver) {
        if (status != RunStatus.PLANNED) {
            return false;
        }
        startedAt = now;
        outstanding.set(toDeliver);
        status = toDeliver == 0 ? RunStatus.COMPLETED : RunStatus.RUNNING;
        if (toDeliver == 0) {
            finishedAt = now;
        }
        return true;
    }

    synchronized void track(Future<?> future) {
        scheduled.add(future);
    }

    void record(DeliveryAttempt attempt) {
        attempts.add(attempt);
    }

    synchronized void deliveryFinished(Instant now) {
        if (outstanding.decrementAndGet() == 0 && status == RunStatus.RUNNING) {
            status = RunStatus.COMPLETED;
            finishedAt = now;
        }
    }

    synchronized boolean cancel(Instant now) {
        if (status == RunStatus.COMPLETED || status == RunStatus.CANCELLED) {
            return false;
        }
        status = RunStatus.CANCELLED;
        finishedAt = now;
        scheduled.forEach(f -> f.cancel(false));
        return true;
    }

    boolean isActive() {
        return status == RunStatus.RUNNING;
    }
}
