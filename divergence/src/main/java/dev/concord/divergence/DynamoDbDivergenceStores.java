package dev.concord.divergence;

import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import dev.concord.resolver.merchant.MerchantOrders;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

/**
 * Incidents and detector state in the payments table, inside each payment's partition:
 * {@code SK=INCIDENT#<type>} and {@code SK=DETECTOR}. Open incidents join the sparse "needs attention"
 * index GSI2 under {@code gsi2pk=INCIDENT}, with a sort key that orders them by severity then score,
 * so the dashboard's "worst first" list is one Query read backwards.
 */
public final class DynamoDbDivergenceStores implements IncidentStore, DetectorStateStore {

    private static final String CURSOR_PK = "DETECTOR#MERCHANT_FEED";

    private final DynamoDbClient dynamo;
    private final String table;

    public DynamoDbDivergenceStores(DynamoDbClient dynamo, String table) {
        this.dynamo = dynamo;
        this.table = table;
    }

    // ---------- incidents ----------

    @Override
    public Optional<Incident> find(String paymentKey, DivergenceType type) {
        return get("PAY#" + paymentKey, "INCIDENT#" + type).map(DynamoDbDivergenceStores::toIncident);
    }

    @Override
    public void put(Incident i) {
        Map<String, AttributeValue> m = new HashMap<>();
        m.put(DynamoDbPaymentStore.PK, s("PAY#" + i.paymentKey()));
        m.put(DynamoDbPaymentStore.SK, s("INCIDENT#" + i.type()));
        m.put("gateway", s(i.gateway().name()));
        m.put("payment_id", s(i.paymentId()));
        m.put("type", s(i.type().name()));
        m.put("status", s(i.status().name()));
        m.put("severity", s(i.severity().name()));
        m.put("score", n(i.score()));
        m.put("first_seen_at", s(i.firstSeenAt().toString()));
        m.put("raised_at", s(i.raisedAt().toString()));
        m.put("updated_at", s(i.updatedAt().toString()));
        m.put("explanation", s(i.explanation()));
        m.put("occurrences", n(i.occurrences()));
        opt(m, "order_id", i.orderId());
        opt(m, "currency", i.currency());
        opt(m, "merchant_status", i.merchantStatus());
        opt(m, "resolution", i.resolution());
        opt(m, "gateway_state", i.gatewayState() == null ? null : i.gatewayState().name());
        opt(m, "concord_state", i.concordState() == null ? null : i.concordState().name());
        opt(m, "resolved_at", i.resolvedAt() == null ? null : i.resolvedAt().toString());
        if (i.amountMinor() != null) {
            m.put("amount_minor", n(i.amountMinor()));
        }
        if (i.open()) {
            m.put(DynamoDbPaymentStore.GSI2_PK, s("INCIDENT"));
            m.put(DynamoDbPaymentStore.GSI2_SK, s(String.format("%d#%019d#%s", i.severity().ordinal(), i.score(), i.paymentKey())));
        }
        dynamo.putItem(b -> b.tableName(table).item(m));
    }

    @Override
    public List<Incident> forPayment(String paymentKey) {
        return dynamo.query(QueryRequest.builder().tableName(table).consistentRead(true)
                        .keyConditionExpression(DynamoDbPaymentStore.PK + " = :pk AND begins_with(" + DynamoDbPaymentStore.SK + ", :p)")
                        .expressionAttributeValues(Map.of(":pk", s("PAY#" + paymentKey), ":p", s("INCIDENT#"))).build())
                .items().stream().map(DynamoDbDivergenceStores::toIncident).toList();
    }

    @Override
    public List<Incident> open(int limit) {
        return dynamo.query(QueryRequest.builder().tableName(table).indexName(DynamoDbPaymentStore.GSI2)
                        .keyConditionExpression(DynamoDbPaymentStore.GSI2_PK + " = :p")
                        .expressionAttributeValues(Map.of(":p", s("INCIDENT")))
                        .scanIndexForward(false).limit(limit).build())
                .items().stream().map(DynamoDbDivergenceStores::toIncident).toList();
    }

    private static Incident toIncident(Map<String, AttributeValue> m) {
        return new Incident(Gateway.valueOf(m.get("gateway").s()), m.get("payment_id").s(), str(m, "order_id"),
                DivergenceType.valueOf(m.get("type").s()), Incident.Status.valueOf(m.get("status").s()),
                Severity.valueOf(m.get("severity").s()), Long.parseLong(m.get("score").n()),
                m.containsKey("amount_minor") ? Long.parseLong(m.get("amount_minor").n()) : null, str(m, "currency"),
                m.containsKey("gateway_state") ? CanonicalState.valueOf(m.get("gateway_state").s()) : null,
                m.containsKey("concord_state") ? CanonicalState.valueOf(m.get("concord_state").s()) : null,
                str(m, "merchant_status"), Instant.parse(m.get("first_seen_at").s()), Instant.parse(m.get("raised_at").s()),
                Instant.parse(m.get("updated_at").s()), m.containsKey("resolved_at") ? Instant.parse(m.get("resolved_at").s()) : null,
                str(m, "resolution"), m.get("explanation").s(), Integer.parseInt(m.get("occurrences").n()));
    }

    // ---------- detector state ----------

    @Override
    public Optional<DetectorState> get(String paymentKey) {
        return get("PAY#" + paymentKey, "DETECTOR").map(m -> {
            Map<DivergenceType, Instant> suspected = new EnumMap<>(DivergenceType.class);
            if (m.containsKey("suspected")) {
                m.get("suspected").m().forEach((k, v) -> suspected.put(DivergenceType.valueOf(k), Instant.parse(v.s())));
            }
            return new DetectorState(paymentKey, m.containsKey("last_checked_at") ? Instant.parse(m.get("last_checked_at").s()) : null,
                    suspected);
        });
    }

    @Override
    public void put(DetectorState st) {
        Map<String, AttributeValue> suspected = new HashMap<>();
        st.suspectedSince().forEach((k, v) -> suspected.put(k.name(), s(v.toString())));
        Map<String, AttributeValue> m = new HashMap<>();
        m.put(DynamoDbPaymentStore.PK, s("PAY#" + st.paymentKey()));
        m.put(DynamoDbPaymentStore.SK, s("DETECTOR"));
        m.put("suspected", AttributeValue.fromM(suspected));
        opt(m, "last_checked_at", st.lastCheckedAt() == null ? null : st.lastCheckedAt().toString());
        dynamo.putItem(b -> b.tableName(table).item(m));
    }

    @Override
    public MerchantOrders.Cursor merchantCursor() {
        return get(CURSOR_PK, "CURSOR").map(m -> new MerchantOrders.Cursor(m.get("since").s(), m.get("after").s()))
                .orElse(MerchantOrders.Cursor.start());
    }

    @Override
    public void saveMerchantCursor(MerchantOrders.Cursor c) {
        dynamo.putItem(b -> b.tableName(table).item(Map.of(DynamoDbPaymentStore.PK, s(CURSOR_PK),
                DynamoDbPaymentStore.SK, s("CURSOR"), "since", s(c.since()), "after", s(c.after()))));
    }

    // ---------- helpers ----------

    private Optional<Map<String, AttributeValue>> get(String pk, String sk) {
        Map<String, AttributeValue> item = dynamo.getItem(b -> b.tableName(table).consistentRead(true)
                .key(Map.of(DynamoDbPaymentStore.PK, s(pk), DynamoDbPaymentStore.SK, s(sk)))).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(item);
    }

    private static void opt(Map<String, AttributeValue> m, String k, String v) {
        if (v != null) {
            m.put(k, s(v));
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
