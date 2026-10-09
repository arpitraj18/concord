package dev.concord.divergence;

import dev.concord.engine.StateEngine;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.engine.store.PaymentStore;
import dev.concord.ingest.Gateway;
import dev.concord.resolver.merchant.MerchantOrder;
import dev.concord.resolver.merchant.MerchantOrders;
import dev.concord.resolver.status.StatusClient;
import dev.concord.resolver.status.StatusResult;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catches mismatches neither system can see alone, by comparing three views of each payment: the
 * gateway's status API (truth), Concord's state, and the merchant's order (through its adapter).
 *
 * <p>Batch or streaming? Both. Event-only checks miss the case where nothing happens (the webhook that
 * never came, the order a timeout job cancelled quietly); a nightly batch is far too slow. So:
 * <ul>
 *   <li><b>event-triggered</b>: every outbox change (Concord learned something) and every merchant-side
 *       change (read from the merchant's keyset-paginated change feed) schedules a check of that payment;</li>
 *   <li><b>periodic sweep</b>: payments changed within a time window are read page by page from the
 *       state + updated_at index, skipping any checked within {@code recheckInterval}.</li>
 * </ul>
 *
 * A divergence becomes an incident only if it is still there {@code grace} after it was first seen,
 * so an event still being delivered is not reported. Incidents are raised once per payment and type,
 * re-scored on every check (severity grows with age), and resolved automatically when the divergence
 * goes away. A CONCORD_DRIFT also flags the payment, so the resolver re-checks it.
 */
public final class DivergenceDetector {

    private static final Logger log = LoggerFactory.getLogger(DivergenceDetector.class);

    private final StateEngine engine;
    private final PaymentStore payments;
    private final StatusClient status;
    private final MerchantOrders merchant;
    private final IncidentStore incidents;
    private final DetectorStateStore states;
    private final DetectorConfig config;
    private final Clock clock;
    private final Map<String, Instant> due = new ConcurrentHashMap<>();

    public DivergenceDetector(StateEngine engine, StatusClient status, MerchantOrders merchant, IncidentStore incidents,
                              DetectorStateStore states, DetectorConfig config, Clock clock) {
        this.engine = engine;
        this.payments = engine.store();
        this.status = status;
        this.merchant = merchant;
        this.incidents = incidents;
        this.states = states;
        this.config = config;
        this.clock = clock;
    }

    public record CheckResult(Set<DivergenceType> found, List<Incident> raised, List<Incident> resolved) {
    }

    // ---------- triggers ----------

    /** Schedule a check of a payment at {@code at} (the earliest request wins). */
    public void trigger(Gateway gateway, String paymentId, Instant at) {
        due.merge(gateway + "#" + paymentId, at, (a, b) -> a.isBefore(b) ? a : b);
    }

    /** Hook for the outbox feed: Concord learned something about this payment. */
    public void onOutbox(OutboxEntry entry) {
        trigger(entry.gateway(), entry.paymentId(), clock.instant().plus(config.settleDelay()));
    }

    /** Reads the merchant's change feed from the saved cursor and schedules a check per changed order. */
    public int pollMerchantChanges() {
        int changed = 0;
        MerchantOrders.Cursor cursor = states.merchantCursor();
        for (int page = 0; page < 50; page++) {
            MerchantOrders.Page p = merchant.changedSince(cursor, config.pageSize());
            for (MerchantOrder o : p.orders()) {
                Optional<Gateway> g = Gateway.fromPath(o.gateway());
                if (g.isPresent() && o.paymentId() != null) {
                    trigger(g.get(), o.paymentId(), clock.instant());
                    changed++;
                }
            }
            cursor = p.next();
            states.saveMerchantCursor(cursor);
            if (p.orders().size() < config.pageSize()) {
                break;
            }
        }
        return changed;
    }

    /** Checks every payment whose scheduled time has come. */
    public int runDue() {
        Instant now = clock.instant();
        int checked = 0;
        for (Map.Entry<String, Instant> e : List.copyOf(due.entrySet())) {
            if (!e.getValue().isAfter(now) && due.remove(e.getKey(), e.getValue())) {
                String[] parts = e.getKey().split("#", 2);
                payments.find(Gateway.valueOf(parts[0]), parts[1]).ifPresent(this::check);
                checked++;
            }
        }
        return checked;
    }

    /** Periodic windowed sweep over every state, page by page, skipping recently checked payments. */
    public int sweep() {
        Instant now = clock.instant();
        int[] checked = {0};
        for (CanonicalState s : CanonicalState.values()) {
            payments.forEachInStateBetween(s, now.minus(config.window()), now, config.pageSize(), r -> {
                Instant last = states.get(r.key()).map(DetectorState::lastCheckedAt).orElse(null);
                if (last == null || Duration.between(last, now).compareTo(config.recheckInterval()) >= 0) {
                    check(r);
                    checked[0]++;
                }
            });
        }
        return checked[0];
    }

    // ---------- the check ----------

    public CheckResult check(PaymentRecord record) {
        Instant now = clock.instant();
        CanonicalState gatewayState = status.fetch(record.gateway(), record.paymentId()) instanceof StatusResult.Found f
                ? f.state() : null;
        MerchantOrder order = record.orderId() == null ? null : merchant.getOrder(record.orderId()).orElse(null);
        Observation obs = new Observation(record, gatewayState, order);
        Set<DivergenceType> found = Classifier.classify(obs);

        DetectorState previous = states.get(record.key()).orElse(DetectorState.fresh(record.key()));
        Map<DivergenceType, Instant> suspected = new EnumMap<>(DivergenceType.class);
        List<Incident> raised = new ArrayList<>();
        List<Incident> resolved = new ArrayList<>();

        for (DivergenceType type : found) {
            Instant since = previous.suspectedSince().getOrDefault(type, now);
            suspected.put(type, since);
            Optional<Incident> existing = incidents.find(record.key(), type).filter(Incident::open);
            if (existing.isPresent() || Duration.between(since, now).compareTo(config.grace()) >= 0) {
                Incident inc = incident(obs, type, since, existing, incidents.find(record.key(), type), now);
                incidents.put(inc);
                if (existing.isEmpty()) {
                    raised.add(inc);
                    log.warn("incident {} {} on {}: {}", inc.severity(), type, record.key(), inc.explanation());
                    if (type == DivergenceType.CONCORD_DRIFT) {
                        engine.flag(record.gateway(), record.paymentId(), "divergence detector: gateway says "
                                + gatewayState + ", Concord has " + record.state());
                    }
                }
            } else {
                // seen, not yet old enough: look again right after the grace period
                trigger(record.gateway(), record.paymentId(), since.plus(config.grace()));
            }
        }
        for (Incident open : incidents.forPayment(record.key())) {
            if (open.open() && !found.contains(open.type())) {
                Incident r = new Incident(open.gateway(), open.paymentId(), open.orderId(), open.type(), Incident.Status.RESOLVED,
                        open.severity(), open.score(), open.amountMinor(), open.currency(), gatewayState, record.state(),
                        order == null ? null : order.status(), open.firstSeenAt(), open.raisedAt(), now, now,
                        "divergence no longer present (gateway " + gatewayState + ", Concord " + record.state()
                                + ", order " + (order == null ? "unknown" : order.status()) + ")",
                        open.explanation(), open.occurrences());
                incidents.put(r);
                resolved.add(r);
            }
        }
        states.put(new DetectorState(record.key(), now, suspected));
        return new CheckResult(found, raised, resolved);
    }

    private static Incident incident(Observation o, DivergenceType type, Instant since, Optional<Incident> open,
                                     Optional<Incident> previous, Instant now) {
        PaymentRecord r = o.concord();
        long amount = r.amountMinor() == null ? 0 : r.amountMinor();
        long score = Severity.score(amount, type, Duration.between(since, now));
        int occurrences = open.map(Incident::occurrences).orElse(previous.map(p -> p.occurrences() + 1).orElse(1));
        return new Incident(r.gateway(), r.paymentId(), r.orderId(), type, Incident.Status.OPEN, Severity.of(score), score,
                r.amountMinor(), r.currency(), o.gatewayState(), r.state(), o.order() == null ? null : o.order().status(),
                since, open.map(Incident::raisedAt).orElse(now), now, null, null, explain(o, type), occurrences);
    }

    /** Plain-language summary; the AI layer (Phase 5) will write richer ones from the payment's history. */
    static String explain(Observation o, DivergenceType type) {
        PaymentRecord r = o.concord();
        String money = r.amountMinor() == null ? "an unknown amount"
                : BigDecimal.valueOf(r.amountMinor(), 2).toPlainString() + " " + (r.currency() == null ? "" : r.currency());
        String truth = "gateway " + r.gateway() + " says " + o.truth();
        String order = o.order() == null ? "" : "order " + o.order().orderId() + " is " + o.order().status();
        String what = switch (type) {
            case PAID_BUT_CANCELLED -> "The customer was charged " + money + " (" + truth + ") but " + order + ".";
            case UNPAID_BUT_FULFILLED -> "The " + order + " but the payment of " + money + " never arrived (" + truth + ").";
            case REFUNDED_BUT_ACTIVE -> money + " was refunded (" + truth + ") but " + order + ".";
            case PAID_BUT_AWAITING -> money + " was captured (" + truth + ") but " + order + ": the merchant missed the update.";
            case AMOUNT_MISMATCH -> "The " + order + " for " + o.order().amountMinor() + " but " + r.amountMinor()
                    + " was captured (" + truth + ").";
            case CONCORD_DRIFT -> "Concord has " + r.state() + " but " + truth + ".";
        };
        return what + " " + type.suggestedAction();
    }
}
