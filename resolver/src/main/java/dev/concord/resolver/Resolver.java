package dev.concord.resolver;

import dev.concord.engine.ProcessResult;
import dev.concord.engine.StateEngine;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.machine.Decision;
import dev.concord.engine.machine.StateMachine;
import dev.concord.engine.mapping.CanonicalEvent;
import dev.concord.engine.mapping.EventSource;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.engine.store.PaymentStore;
import dev.concord.resolver.SweepReport.Action;
import dev.concord.resolver.merchant.MerchantOrders;
import dev.concord.resolver.state.ResolverState;
import dev.concord.resolver.state.ResolverStateStore;
import dev.concord.resolver.status.GatewayActions;
import dev.concord.resolver.status.StatusClient;
import dev.concord.resolver.status.StatusResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes sure no payment stays uncertain or wrong for long, using the gateway's status API as the
 * source of truth. Run periodically (EventBridge Scheduler in AWS); every sweep:
 *
 * <ol>
 *   <li>collects candidates: flagged payments (sparse index); payments sitting in CREATED,
 *       PENDING, UNKNOWN or AUTHORIZED too long (state + updated_at index); CAPTURED ones awaiting
 *       settlement on gateways that report it; and every payment that recently reached a final state,
 *       verified once per version, because a lie that fits a legal path (a fake "captured") looks
 *       exactly like the truth from webhooks alone. Highest amounts first.</li>
 *   <li>for each one that is due, asks the status API and compares:
 *     <ul>
 *       <li>truth is <b>ahead</b>: apply it through the normal engine path (a webhook was dropped);</li>
 *       <li>truth <b>equals</b> Concord: confirm, clear any flag; act on long-authorized payments;</li>
 *       <li>truth is <b>behind or on another branch</b>: Concord was told a lie. Correct it, but only
 *           after a second observation at least {@code confirmDelay} later still disagrees and the
 *           payment has not changed in between (status APIs can lag behind webhooks);</li>
 *       <li><b>no answer</b>: retry with exponential backoff and full jitter; a PENDING payment that
 *           stays unreachable becomes UNKNOWN; a payment unresolved for {@code parkAfter} is parked
 *           for a human.</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * Every write goes through the same conditional engine write as webhooks, so a webhook and a poll
 * racing for one payment need no special handling: the first commit wins.
 */
public final class Resolver {

    private static final Logger log = LoggerFactory.getLogger(Resolver.class);
    private static final List<CanonicalState> UNCERTAIN = List.of(
            CanonicalState.CREATED, CanonicalState.PENDING, CanonicalState.UNKNOWN, CanonicalState.AUTHORIZED);
    private static final List<CanonicalState> FINAL = List.of(
            CanonicalState.CAPTURED, CanonicalState.SETTLED, CanonicalState.FAILED, CanonicalState.REFUNDED);

    private final StateEngine engine;
    private final PaymentStore store;
    private final Normalizer normalizer;
    private final StatusClient status;
    private final GatewayActions actions;
    private final MerchantOrders merchant;
    private final ResolverStateStore states;
    private final ResolverConfig config;
    private final Clock clock;

    public Resolver(StateEngine engine, Normalizer normalizer, StatusClient status, GatewayActions actions,
                    MerchantOrders merchant, ResolverStateStore states, ResolverConfig config, Clock clock) {
        this.engine = engine;
        this.store = engine.store();
        this.normalizer = normalizer;
        this.status = status;
        this.actions = actions;
        this.merchant = merchant;
        this.states = states;
        this.config = config;
        this.clock = clock;
    }

    public SweepReport sweep() {
        Instant now = clock.instant();
        SweepReport report = new SweepReport();
        Map<String, PaymentRecord> candidates = new LinkedHashMap<>();
        store.findFlagged(config.batchSize()).forEach(r -> candidates.put(r.key(), r));
        for (CanonicalState s : UNCERTAIN) {
            store.findInStateSince(s, now.minus(config.checkAfter()), config.batchSize()).forEach(r -> candidates.putIfAbsent(r.key(), r));
        }
        store.findInStateSince(CanonicalState.CAPTURED, now.minus(config.settleCheckAfter()), config.batchSize()).stream()
                .filter(r -> normalizer.statusStates(r.gateway()).contains(CanonicalState.SETTLED))
                .forEach(r -> candidates.putIfAbsent(r.key(), r));
        for (CanonicalState s : FINAL) {
            store.findInStateBetween(s, now.minus(config.verifyWindow()), now.minus(config.verifyFinalAfter()), config.batchSize())
                    .stream().filter(r -> !alreadyConfirmed(r))
                    .forEach(r -> candidates.putIfAbsent(r.key(), r));
        }

        candidates.values().stream()
                .sorted(Comparator.comparing((PaymentRecord r) -> r.amountMinor() == null ? 0 : r.amountMinor()).reversed())
                .forEach(r -> resolve(r, now, report));
        return report;
    }

    private boolean alreadyConfirmed(PaymentRecord r) {
        return states.get(r.key()).map(rs -> rs.confirmedVersion() == r.version() && !r.needsVerification()).orElse(false)
                && !(r.state() == CanonicalState.CAPTURED && normalizer.statusStates(r.gateway()).contains(CanonicalState.SETTLED));
    }

    /** Checks one payment now (also used by tests and by an operator "check this payment" action). */
    public void resolve(PaymentRecord record, Instant now, SweepReport report) {
        ResolverState rs = states.get(record.key()).orElse(ResolverState.fresh(record.key()));
        if (rs.parked()) {
            return;
        }
        if (rs.nextCheckAt() != null && now.isBefore(rs.nextCheckAt())) {
            report.add(Action.SKIPPED_NOT_DUE);
            return;
        }
        report.add(Action.CHECKED);
        StatusResult result = status.fetch(record.gateway(), record.paymentId());
        switch (result) {
            case StatusResult.Found f -> found(record, rs, f, now, report);
            case StatusResult.NotFound n -> noAnswer(record, rs, "not found at gateway", now, report);
            case StatusResult.Unavailable u -> noAnswer(record, rs, u.reason(), now, report);
        }
    }

    private void found(PaymentRecord record, ResolverState rs, StatusResult.Found f, Instant now, SweepReport report) {
        rs = rs.checked(now, f.state().name());
        CanonicalState truth = f.state();

        // UNKNOWN is Concord's own "cannot tell", not a claim: the gateway saying PENDING just means "still pending".
        if (record.state() == CanonicalState.UNKNOWN && truth == CanonicalState.PENDING) {
            states.put(scheduleRetry(rs, now));
            return;
        }

        Decision d = StateMachine.decide(record.state(), truth);
        switch (d.kind()) {
            case APPLY -> {
                ProcessResult r = engine.process(statusEvent(record, truth, now));
                if (r.record() != null && r.record().state() == truth && r.record().needsVerification()) {
                    engine.verify(record.gateway(), record.paymentId(), truth, "status api: " + f.raw());
                }
                report.add(Action.APPLIED);
                states.put(rs.progressed());
            }
            case SAME -> {
                rs = rs.confirmed(record.needsVerification() ? record.version() + 1 : record.version());
                if (record.needsVerification()) {
                    engine.verify(record.gateway(), record.paymentId(), truth, "status api: " + f.raw());
                    report.add(Action.VERIFIED);
                } else {
                    report.add(Action.CONFIRMED);
                }
                if (truth == CanonicalState.AUTHORIZED && !rs.actionTaken()
                        && Duration.between(record.createdAt(), now).compareTo(config.actOnAuthorizedAfter()) >= 0) {
                    rs = actOnAuthorized(record, rs, report);
                }
                boolean settled = truth.isTerminal() || truth == CanonicalState.SETTLED
                        || (truth == CanonicalState.CAPTURED && !normalizer.statusStates(record.gateway()).contains(CanonicalState.SETTLED));
                states.put(settled ? rs.progressed() : scheduleRetry(rs, now));
            }
            case STALE, CONTRADICTION -> suspectWrong(record, rs, truth, f.raw(), now, report);
        }
    }

    /** Concord's state is ahead of, or on another branch from, the truth. Correct only after a confirmation. */
    private void suspectWrong(PaymentRecord record, ResolverState rs, CanonicalState truth, String raw, Instant now,
                              SweepReport report) {
        boolean confirmed = rs.pendingCorrection() == truth
                && rs.pendingCorrectionVersion() == record.version()
                && Duration.between(rs.pendingCorrectionSince(), now).compareTo(config.confirmDelay()) >= 0;
        if (!confirmed) {
            report.add(Action.SUSPECTED_WRONG);
            // Same observation as before keeps its original time; a new one (or a changed payment) restarts.
            Instant since = rs.pendingCorrection() == truth && rs.pendingCorrectionVersion() == record.version()
                    ? rs.pendingCorrectionSince() : now;
            states.put(rs.suspectCorrection(truth, since, record.version(), since.plus(config.confirmDelay())));
            return;
        }
        ProcessResult r = engine.correct(record.gateway(), record.paymentId(), truth, record.version(),
                "status api reported " + truth + " twice, " + config.confirmDelay().toSeconds() + "s apart, while Concord had "
                        + record.state() + (record.needsVerification() ? " (" + record.verificationReason() + ")" : "")
                        + "; last response " + raw);
        if (r.outcome() != null) {
            report.add(Action.CORRECTED);
            states.put(rs.progressed());
        } else {
            // The payment changed between the two observations: look again from scratch.
            states.put(rs.progressed());
        }
    }

    private void noAnswer(PaymentRecord record, ResolverState rs, String reason, Instant now, SweepReport report) {
        report.add(Action.UNAVAILABLE);
        rs = rs.checked(now, "no answer: " + reason);
        if (record.state() == CanonicalState.PENDING
                && Duration.between(record.updatedAt(), now).compareTo(config.unknownAfter()) >= 0) {
            ProcessResult r = engine.process(new CanonicalEvent(record.gateway(), record.paymentId(),
                    "TIMEOUT#" + record.key() + "#v" + record.version(), "concord.timeout", CanonicalState.UNKNOWN,
                    null, null, null, null, now, "concord", null, EventSource.TIMEOUT));
            if (r.outcome() != null) {
                report.add(Action.MARKED_UNKNOWN);
            }
        }
        ResolverState next = scheduleRetry(rs, now);
        Instant unknownDeadline = record.updatedAt().plus(config.unknownAfter());
        if (record.state() == CanonicalState.PENDING && !next.parked() && next.nextCheckAt().isAfter(unknownDeadline)
                && unknownDeadline.isAfter(now)) {
            // Never let backoff jitter delay telling the merchant "uncertain, do not cancel".
            next = next.retryAt(unknownDeadline, next.attempts());
        }
        if (next.parked()) {
            report.add(Action.PARKED);
        }
        states.put(next);
    }

    /**
     * Authorized for too long: the money is held on the customer's card but nobody captured it.
     * Ask the merchant whether the order still wants it: capture (save the sale) or release (money back
     * in minutes instead of days). Done once per payment; the result goes through the normal engine path.
     */
    private ResolverState actOnAuthorized(PaymentRecord record, ResolverState rs, SweepReport report) {
        if (record.orderId() == null) {
            return rs;
        }
        MerchantOrders.OrderStatus order = merchant.orderStatus(record.orderId());
        CanonicalState after = switch (order) {
            case VALID -> actions.capture(record.gateway(), record.paymentId());
            case CANCELLED -> actions.release(record.gateway(), record.paymentId());
            case UNKNOWN -> null;
        };
        if (after == null) {
            return rs;
        }
        report.add(order == MerchantOrders.OrderStatus.VALID ? Action.CAPTURED : Action.RELEASED);
        engine.process(statusEvent(record, after, clock.instant()));
        return rs.withAction();
    }

    private ResolverState scheduleRetry(ResolverState rs, Instant now) {
        int attempts = rs.attempts() + 1;
        if (Duration.between(rs.unresolvedSince(), now).compareTo(config.parkAfter()) >= 0) {
            log.warn("parking {}: unresolved since {} after {} checks (last: {})", rs.paymentKey(), rs.unresolvedSince(),
                    attempts, rs.lastResult());
            return rs.park("unresolved since " + rs.unresolvedSince() + " after " + attempts + " checks (last: "
                    + rs.lastResult() + ")");
        }
        return rs.retryAt(now.plus(backoff(attempts)), attempts);
    }

    /** Exponential backoff with full jitter: random in [base, min(max, base * 2^attempt)]. Spreads retries out. */
    Duration backoff(int attempt) {
        long base = config.backoffBase().toMillis();
        long cap = Math.min(config.backoffMax().toMillis(), base * (1L << Math.min(20, attempt)));
        return Duration.ofMillis(base + ThreadLocalRandom.current().nextLong(Math.max(1, cap - base + 1)));
    }

    private static CanonicalEvent statusEvent(PaymentRecord record, CanonicalState state, Instant now) {
        return new CanonicalEvent(record.gateway(), record.paymentId(),
                "POLL#" + record.key() + "#" + state + "#v" + record.version(), "status_api." + state, state,
                null, null, null, null, now, "status", null, EventSource.STATUS_API);
    }
}
