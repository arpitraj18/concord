package dev.concord.resolver.state;

import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.machine.CanonicalState;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

/**
 * Lives in the payment's own partition ({@code SK=RESOLVER}) but is a separate item, so resolver
 * scheduling never touches the payment's version. Parked items join the sparse GSI2 under
 * {@code gsi2pk=PARKED}: GSI2 is the "needs attention" index (VERIFY for flagged payments, PARKED for
 * payments a human must look at).
 */
public final class DynamoDbResolverStateStore implements ResolverStateStore {

    private final DynamoDbClient dynamo;
    private final String table;

    public DynamoDbResolverStateStore(DynamoDbClient dynamo, String table) {
        this.dynamo = dynamo;
        this.table = table;
    }

    @Override
    public Optional<ResolverState> get(String paymentKey) {
        Map<String, AttributeValue> item = dynamo.getItem(b -> b.tableName(table).consistentRead(true)
                .key(Map.of(DynamoDbPaymentStore.PK, s("PAY#" + paymentKey), DynamoDbPaymentStore.SK, s("RESOLVER")))).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(from(item));
    }

    @Override
    public void put(ResolverState st) {
        Map<String, AttributeValue> m = new HashMap<>();
        m.put(DynamoDbPaymentStore.PK, s("PAY#" + st.paymentKey()));
        m.put(DynamoDbPaymentStore.SK, s("RESOLVER"));
        m.put("payment_key", s(st.paymentKey()));
        m.put("attempts", n(st.attempts()));
        m.put("parked", AttributeValue.fromBool(st.parked()));
        m.put("action_taken", AttributeValue.fromBool(st.actionTaken()));
        m.put("pending_correction_version", n(st.pendingCorrectionVersion()));
        m.put("confirmed_version", n(st.confirmedVersion()));
        opt(m, "unresolved_since", st.unresolvedSince());
        opt(m, "next_check_at", st.nextCheckAt());
        opt(m, "pending_correction_since", st.pendingCorrectionSince());
        opt(m, "last_checked_at", st.lastCheckedAt());
        if (st.pendingCorrection() != null) {
            m.put("pending_correction", s(st.pendingCorrection().name()));
        }
        if (st.parkedReason() != null) {
            m.put("parked_reason", s(st.parkedReason()));
        }
        if (st.lastResult() != null) {
            m.put("last_result", s(st.lastResult()));
        }
        if (st.parked()) {
            m.put(DynamoDbPaymentStore.GSI2_PK, s("PARKED"));
            m.put(DynamoDbPaymentStore.GSI2_SK, s(String.valueOf(st.lastCheckedAt())));
        }
        dynamo.putItem(b -> b.tableName(table).item(m));
    }

    @Override
    public List<ResolverState> parked() {
        return dynamo.queryPaginator(QueryRequest.builder().tableName(table).indexName(DynamoDbPaymentStore.GSI2)
                        .keyConditionExpression(DynamoDbPaymentStore.GSI2_PK + " = :p")
                        .expressionAttributeValues(Map.of(":p", s("PARKED"))).build())
                .items().stream().map(DynamoDbResolverStateStore::from).toList();
    }

    private static ResolverState from(Map<String, AttributeValue> m) {
        return new ResolverState(
                m.get("payment_key").s(),
                Integer.parseInt(m.get("attempts").n()),
                inst(m, "unresolved_since"),
                inst(m, "next_check_at"),
                m.containsKey("pending_correction") ? CanonicalState.valueOf(m.get("pending_correction").s()) : null,
                inst(m, "pending_correction_since"),
                Long.parseLong(m.get("pending_correction_version").n()),
                m.get("parked").bool(),
                m.containsKey("parked_reason") ? m.get("parked_reason").s() : null,
                m.get("action_taken").bool(),
                inst(m, "last_checked_at"),
                m.containsKey("last_result") ? m.get("last_result").s() : null,
                Long.parseLong(m.get("confirmed_version").n()));
    }

    private static void opt(Map<String, AttributeValue> m, String k, Instant v) {
        if (v != null) {
            m.put(k, s(v.toString()));
        }
    }

    private static Instant inst(Map<String, AttributeValue> m, String k) {
        return m.containsKey(k) ? Instant.parse(m.get(k).s()) : null;
    }

    private static AttributeValue s(String v) {
        return AttributeValue.fromS(v);
    }

    private static AttributeValue n(long v) {
        return AttributeValue.fromN(Long.toString(v));
    }
}
