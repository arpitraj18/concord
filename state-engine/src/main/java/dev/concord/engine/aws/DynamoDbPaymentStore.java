package dev.concord.engine.aws;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.store.AlreadyProcessedException;
import dev.concord.engine.store.Commit;
import dev.concord.engine.store.EventOutcome;
import dev.concord.engine.store.HistoryEntry;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.engine.store.PaymentStore;
import dev.concord.engine.store.VersionConflictException;
import dev.concord.ingest.Gateway;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/**
 * Single-table design. One partition per payment holds everything about it:
 *
 * <pre>
 * PK = PAY#&lt;gateway&gt;#&lt;paymentId&gt;   SK = STATE              current state, version, amount, flags
 *                                       SK = EVT#&lt;dedupKey&gt;     one item per event (history)
 *                                       SK = OUTBOX#&lt;version&gt;   state changes to deliver (Phase 3)
 * GSI1: gsi1pk = STATE#&lt;state&gt;, gsi1sk = updated_at          -> "payments stuck in PENDING since T"
 * </pre>
 *
 * The gateway is part of the key because payment ids are only unique within one gateway. Reading a
 * payment with its whole history is one Query on one partition.
 *
 * <p>Each event is one {@code TransactWriteItems}: the STATE item (or a ConditionCheck on it) guarded by
 * the expected version, the EVT item guarded by "must not exist", and the OUTBOX item. All or nothing,
 * so state and outbox can never disagree.
 */
public final class DynamoDbPaymentStore implements PaymentStore {

    public static final String PK = "pk";
    public static final String SK = "sk";
    public static final String GSI1 = "gsi1";
    public static final String GSI1_PK = "gsi1pk";
    public static final String GSI1_SK = "gsi1sk";
    private static final String STATE_SK = "STATE";

    private final DynamoDbClient dynamo;
    private final String table;

    public DynamoDbPaymentStore(DynamoDbClient dynamo, String table) {
        this.dynamo = dynamo;
        this.table = table;
    }

    static String pk(String paymentKey) {
        return "PAY#" + paymentKey;
    }

    @Override
    public Optional<PaymentRecord> find(Gateway gateway, String paymentId) {
        Map<String, AttributeValue> item = dynamo.getItem(b -> b.tableName(table).consistentRead(true)
                .key(Map.of(PK, s(pk(gateway + "#" + paymentId)), SK, s(STATE_SK)))).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(toRecord(item));
    }

    @Override
    public void commit(Commit commit) {
        String pk = pk(commit.paymentKey());
        List<TransactWriteItem> items = new ArrayList<>();

        String versionCondition = commit.expectedVersion() == 0 ? "attribute_not_exists(" + PK + ")" : "version = :v";
        Map<String, AttributeValue> versionValues = commit.expectedVersion() == 0
                ? null : Map.of(":v", n(commit.expectedVersion()));
        if (commit.newRecord().isPresent()) {
            items.add(TransactWriteItem.builder().put(Put.builder().tableName(table)
                    .item(stateItem(pk, commit.newRecord().get()))
                    .conditionExpression(versionCondition)
                    .expressionAttributeValues(versionValues).build()).build());
        } else {
            items.add(TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(table)
                    .key(Map.of(PK, s(pk), SK, s(STATE_SK)))
                    .conditionExpression(versionCondition)
                    .expressionAttributeValues(versionValues).build()).build());
        }
        items.add(TransactWriteItem.builder().put(Put.builder().tableName(table)
                .item(historyItem(pk, commit.history()))
                .conditionExpression("attribute_not_exists(" + SK + ")").build()).build());
        commit.outbox().ifPresent(o -> items.add(TransactWriteItem.builder().put(Put.builder().tableName(table)
                .item(outboxItem(pk, o))
                .conditionExpression("attribute_not_exists(" + SK + ")").build()).build()));

        try {
            dynamo.transactWriteItems(b -> b.transactItems(items));
        } catch (TransactionCanceledException e) {
            List<CancellationReason> reasons = e.hasCancellationReasons() ? e.cancellationReasons() : List.of();
            if (failed(reasons, 1)) {
                throw new AlreadyProcessedException(commit.history().dedupKey());
            }
            if (failed(reasons, 0)) {
                throw new VersionConflictException(commit.paymentKey());
            }
            throw e;
        }
    }

    private static boolean failed(List<CancellationReason> reasons, int index) {
        return reasons.size() > index && "ConditionalCheckFailed".equals(reasons.get(index).code());
    }

    @Override
    public List<HistoryEntry> history(Gateway gateway, String paymentId) {
        return queryPartition(gateway + "#" + paymentId, "EVT#").stream()
                .map(DynamoDbPaymentStore::toHistory)
                .sorted(Comparator.comparing(HistoryEntry::receivedAt).thenComparing(HistoryEntry::dedupKey))
                .toList();
    }

    @Override
    public List<OutboxEntry> outbox(Gateway gateway, String paymentId) {
        return queryPartition(gateway + "#" + paymentId, "OUTBOX#").stream().map(DynamoDbPaymentStore::toOutbox).toList();
    }

    private List<Map<String, AttributeValue>> queryPartition(String paymentKey, String prefix) {
        QueryRequest request = QueryRequest.builder().tableName(table).consistentRead(true)
                .keyConditionExpression(PK + " = :pk AND begins_with(" + SK + ", :p)")
                .expressionAttributeValues(Map.of(":pk", s(pk(paymentKey)), ":p", s(prefix))).build();
        List<Map<String, AttributeValue>> out = new ArrayList<>();
        dynamo.queryPaginator(request).items().forEach(out::add);
        return out;
    }

    @Override
    public List<PaymentRecord> findInStateSince(CanonicalState state, Instant before, int limit) {
        QueryRequest request = QueryRequest.builder().tableName(table).indexName(GSI1)
                .keyConditionExpression(GSI1_PK + " = :s AND " + GSI1_SK + " < :before")
                .expressionAttributeValues(Map.of(":s", s("STATE#" + state), ":before", s(before.toString())))
                .limit(limit).build();
        return dynamo.query(request).items().stream().map(DynamoDbPaymentStore::toRecord).toList();
    }

    /** Full scan: only for replay comparisons and tests, never on a hot path. */
    @Override
    public List<PaymentRecord> all() {
        List<PaymentRecord> out = new ArrayList<>();
        dynamo.scanPaginator(ScanRequest.builder().tableName(table)
                        .filterExpression(SK + " = :s").expressionAttributeValues(Map.of(":s", s(STATE_SK))).build())
                .items().forEach(i -> out.add(toRecord(i)));
        return out;
    }

    // ---------- item mapping ----------

    private static Map<String, AttributeValue> stateItem(String pk, PaymentRecord r) {
        Map<String, AttributeValue> m = new HashMap<>();
        m.put(PK, s(pk));
        m.put(SK, s(STATE_SK));
        m.put("gateway", s(r.gateway().name()));
        m.put("payment_id", s(r.paymentId()));
        m.put("state", s(r.state().name()));
        m.put("version", n(r.version()));
        m.put("needs_verification", AttributeValue.fromBool(r.needsVerification()));
        m.put("created_at", s(r.createdAt().toString()));
        m.put("updated_at", s(r.updatedAt().toString()));
        m.put(GSI1_PK, s("STATE#" + r.state()));
        m.put(GSI1_SK, s(r.updatedAt().toString()));
        putIfPresent(m, "amount_minor", r.amountMinor() == null ? null : n(r.amountMinor()));
        putIfPresent(m, "currency", r.currency() == null ? null : s(r.currency()));
        putIfPresent(m, "order_id", r.orderId() == null ? null : s(r.orderId()));
        putIfPresent(m, "verification_reason", r.verificationReason() == null ? null : s(r.verificationReason()));
        return m;
    }

    private static PaymentRecord toRecord(Map<String, AttributeValue> m) {
        return new PaymentRecord(
                Gateway.valueOf(m.get("gateway").s()),
                m.get("payment_id").s(),
                CanonicalState.valueOf(m.get("state").s()),
                Long.parseLong(m.get("version").n()),
                m.containsKey("amount_minor") ? Long.parseLong(m.get("amount_minor").n()) : null,
                str(m, "currency"),
                str(m, "order_id"),
                m.get("needs_verification").bool(),
                str(m, "verification_reason"),
                Instant.parse(m.get("created_at").s()),
                Instant.parse(m.get("updated_at").s()));
    }

    private static Map<String, AttributeValue> historyItem(String pk, HistoryEntry h) {
        Map<String, AttributeValue> m = new HashMap<>();
        m.put(PK, s(pk));
        m.put(SK, s("EVT#" + h.dedupKey()));
        m.put("dedup_key", s(h.dedupKey()));
        m.put("event_type", s(h.eventType()));
        m.put("outcome", s(h.outcome().name()));
        m.put("state_before", s(h.stateBefore().name()));
        m.put("state_after", s(h.stateAfter().name()));
        m.put("version_after", n(h.versionAfter()));
        m.put("path", AttributeValue.fromL(h.path().stream().map(p -> s(p.name())).toList()));
        m.put("received_at", s(h.receivedAt().toString()));
        m.put("processed_at", s(h.processedAt().toString()));
        m.put("mapping_id", s(h.mappingId()));
        putIfPresent(m, "claimed_state", h.claimedState() == null ? null : s(h.claimedState().name()));
        putIfPresent(m, "occurred_at", h.occurredAt() == null ? null : s(h.occurredAt().toString()));
        putIfPresent(m, "archive_key", h.archiveKey() == null ? null : s(h.archiveKey()));
        return m;
    }

    private static HistoryEntry toHistory(Map<String, AttributeValue> m) {
        return new HistoryEntry(
                m.get("dedup_key").s(),
                m.get("event_type").s(),
                m.containsKey("claimed_state") ? CanonicalState.valueOf(m.get("claimed_state").s()) : null,
                EventOutcome.valueOf(m.get("outcome").s()),
                CanonicalState.valueOf(m.get("state_before").s()),
                CanonicalState.valueOf(m.get("state_after").s()),
                Long.parseLong(m.get("version_after").n()),
                m.get("path").l().stream().map(v -> CanonicalState.valueOf(v.s())).toList(),
                Instant.parse(m.get("received_at").s()),
                m.containsKey("occurred_at") ? Instant.parse(m.get("occurred_at").s()) : null,
                Instant.parse(m.get("processed_at").s()),
                m.get("mapping_id").s(),
                str(m, "archive_key"));
    }

    private static Map<String, AttributeValue> outboxItem(String pk, OutboxEntry o) {
        return Map.of(
                PK, s(pk),
                SK, s(String.format("OUTBOX#%012d", o.version())),
                "payment_key", s(o.paymentKey()),
                "version", n(o.version()),
                "from_state", s(o.fromState().name()),
                "to_state", s(o.toState().name()),
                "idempotency_key", s(o.idempotencyKey()),
                "created_at", s(o.createdAt().toString()));
    }

    private static OutboxEntry toOutbox(Map<String, AttributeValue> m) {
        return new OutboxEntry(m.get("payment_key").s(), Long.parseLong(m.get("version").n()),
                CanonicalState.valueOf(m.get("from_state").s()), CanonicalState.valueOf(m.get("to_state").s()),
                m.get("idempotency_key").s(), Instant.parse(m.get("created_at").s()));
    }

    private static void putIfPresent(Map<String, AttributeValue> m, String k, AttributeValue v) {
        if (v != null) {
            m.put(k, v);
        }
    }

    private static String str(Map<String, AttributeValue> m, String k) {
        return m.containsKey(k) ? m.get(k).s() : null;
    }

    private static AttributeValue s(String v) {
        return AttributeValue.fromS(v);
    }

    private static AttributeValue n(long v) {
        return AttributeValue.fromN(Long.toString(v));
    }
}
