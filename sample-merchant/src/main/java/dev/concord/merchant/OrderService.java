package dev.concord.merchant;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies Concord's events to orders. Exactly-once effects come from three guards, all inside one
 * database transaction:
 * <ol>
 *   <li>{@code INSERT INTO processed_events ... ON CONFLICT DO NOTHING}: a redelivered event (same
 *       idempotency key) inserts nothing and changes nothing. Two copies arriving at the same instant
 *       serialise on the primary key; the second sees the first's row once it commits.</li>
 *   <li>{@code SELECT ... FOR UPDATE} on the order: events for one order apply one at a time.</li>
 *   <li>{@code payment_version}: an event older than the last one applied is recorded but ignored.</li>
 * </ol>
 * A side effect is emitted only when the order's status actually changes, so two different events
 * that mean "paid" (CAPTURED, then SETTLED) ship the goods once.
 */
@Service
public class OrderService {

    private final JdbcTemplate jdbc;

    public OrderService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record ConcordEvent(String idempotencyKey, String kind, String gateway, String paymentId, String orderId,
                               String state, String previousState, long version, Long amountMinor, String currency,
                               String occurredAt) {
    }

    public enum Outcome { APPLIED, DUPLICATE, STALE, UNKNOWN_ORDER }

    static String orderStatusFor(String paymentState) {
        return switch (paymentState) {
            case "CAPTURED", "SETTLED" -> "PAID";
            case "FAILED" -> "PAYMENT_FAILED";
            case "REFUNDED" -> "REFUNDED";
            default -> "AWAITING_PAYMENT"; // CREATED, PENDING, AUTHORIZED, and UNKNOWN: "uncertain, do not cancel"
        };
    }

    static String effectFor(String from, String to) {
        if (to.equals("PAID")) {
            return "FULFIL";
        }
        if (from.equals("PAID") && (to.equals("AWAITING_PAYMENT") || to.equals("PAYMENT_FAILED"))) {
            return "STOP_FULFILMENT"; // a correction: we were told "paid" and it was not true
        }
        if (to.equals("PAYMENT_FAILED")) {
            return "NOTIFY_PAYMENT_FAILED";
        }
        if (to.equals("REFUNDED")) {
            return "RECORD_REFUND";
        }
        return null;
    }

    @Transactional
    public Outcome apply(ConcordEvent e) {
        int inserted = jdbc.update("INSERT INTO processed_events (idempotency_key, order_id, outcome) VALUES (?, ?, 'APPLIED') "
                + "ON CONFLICT (idempotency_key) DO NOTHING", e.idempotencyKey(), e.orderId());
        jdbc.update("INSERT INTO deliveries (idempotency_key, duplicate) VALUES (?, ?)", e.idempotencyKey(), inserted == 0);
        if (inserted == 0) {
            return Outcome.DUPLICATE;
        }
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT order_id, status, payment_version FROM orders WHERE gateway = ? AND payment_id = ? FOR UPDATE",
                e.gateway(), e.paymentId());
        if (rows.isEmpty()) {
            jdbc.update("UPDATE processed_events SET outcome = 'UNKNOWN_ORDER' WHERE idempotency_key = ?", e.idempotencyKey());
            return Outcome.UNKNOWN_ORDER;
        }
        Map<String, Object> order = rows.getFirst();
        String orderId = (String) order.get("order_id");
        if (e.version() <= ((Number) order.get("payment_version")).longValue()) {
            jdbc.update("UPDATE processed_events SET outcome = 'STALE' WHERE idempotency_key = ?", e.idempotencyKey());
            return Outcome.STALE;
        }
        String oldStatus = (String) order.get("status");
        // A cancelled order stays cancelled; the payment state is still recorded so the mismatch is visible
        // (a captured payment on a cancelled order is exactly what Phase 4's divergence detector looks for).
        String newStatus = oldStatus.equals("CANCELLED") ? "CANCELLED" : orderStatusFor(e.state());
        jdbc.update("UPDATE orders SET status = ?, payment_state = ?, payment_version = ?, updated_at = now() WHERE order_id = ?",
                newStatus, e.state(), e.version(), orderId);
        String effect = newStatus.equals(oldStatus) ? null : effectFor(oldStatus, newStatus);
        if (effect != null) {
            jdbc.update("INSERT INTO effects (order_id, effect, idempotency_key) VALUES (?, ?, ?)", orderId, effect, e.idempotencyKey());
        }
        return Outcome.APPLIED;
    }

    public void create(String orderId, String gateway, String paymentId, long amountMinor, String currency) {
        jdbc.update("INSERT INTO orders (order_id, gateway, payment_id, amount_minor, currency, status) "
                + "VALUES (?, ?, ?, ?, ?, 'AWAITING_PAYMENT') ON CONFLICT (order_id) DO NOTHING",
                orderId, gateway, paymentId, amountMinor, currency);
    }

    /** The merchant gives up on an unpaid order (e.g. a 15-minute checkout timeout). */
    public boolean cancel(String orderId) {
        return jdbc.update("UPDATE orders SET status = 'CANCELLED', updated_at = now() WHERE order_id = ? "
                + "AND status = 'AWAITING_PAYMENT'", orderId) == 1;
    }

    public Optional<Map<String, Object>> find(String orderId) {
        return jdbc.queryForList("SELECT order_id, gateway, payment_id, amount_minor, currency, status, payment_state, "
                + "payment_version FROM orders WHERE order_id = ?", orderId).stream().findFirst();
    }

    public Map<String, Object> stats() {
        return Map.of(
                "deliveriesReceived", count("SELECT count(*) FROM deliveries"),
                "duplicatesAbsorbed", count("SELECT count(*) FROM deliveries WHERE duplicate"),
                "eventsProcessed", count("SELECT count(*) FROM processed_events"),
                "staleIgnored", count("SELECT count(*) FROM processed_events WHERE outcome = 'STALE'"),
                "unknownOrder", count("SELECT count(*) FROM processed_events WHERE outcome = 'UNKNOWN_ORDER'"),
                "effects", count("SELECT count(*) FROM effects"),
                // Must always be 0: one event causing more than one effect.
                "duplicateEffects", count("SELECT count(*) FROM (SELECT idempotency_key FROM effects "
                        + "GROUP BY idempotency_key HAVING count(*) > 1) d"),
                // Must always be 0: goods shipped twice (fulfilments minus stops above 1) or a stop without a fulfilment.
                "ordersFulfilledWrongly", count("SELECT count(*) FROM (SELECT order_id, "
                        + "sum(CASE effect WHEN 'FULFIL' THEN 1 WHEN 'STOP_FULFILMENT' THEN -1 ELSE 0 END) AS net "
                        + "FROM effects GROUP BY order_id) n WHERE net < 0 OR net > 1"));
    }

    private long count(String sql) {
        Long n = jdbc.queryForObject(sql, Long.class);
        return n == null ? 0 : n;
    }
}
