package dev.concord.divergence;

import static org.assertj.core.api.Assertions.assertThat;

import dev.concord.engine.aws.PaymentsTable;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.aws.DynamoDbLocal;
import dev.concord.resolver.merchant.MerchantOrders;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DynamoDbDivergenceStoresTest {

    static Incident incident(String id, Severity sev, long score, Incident.Status status) {
        Instant t = Instant.parse("2026-10-11T10:00:00Z");
        return new Incident(Gateway.A, id, "o_" + id, DivergenceType.PAID_BUT_CANCELLED, status, sev, score, 100L, "INR",
                CanonicalState.CAPTURED, CanonicalState.CAPTURED, "CANCELLED", t, t, t, null, null, "explained", 1);
    }

    @Test
    void incidentsRoundTripAndOpenOnesComeBackWorstFirst() {
        var dynamo = DynamoDbLocal.client();
        PaymentsTable.create(dynamo, "divergence-test");
        DynamoDbDivergenceStores stores = new DynamoDbDivergenceStores(dynamo, "divergence-test");

        stores.put(incident("low", Severity.LOW, 900, Incident.Status.OPEN));
        stores.put(incident("crit", Severity.CRITICAL, 9_000_000, Incident.Status.OPEN));
        stores.put(incident("med", Severity.MEDIUM, 90_000, Incident.Status.OPEN));
        stores.put(incident("med2", Severity.MEDIUM, 400_000, Incident.Status.OPEN));
        stores.put(incident("done", Severity.HIGH, 800_000, Incident.Status.RESOLVED));

        assertThat(stores.open(10)).extracting(Incident::paymentId).containsExactly("crit", "med2", "med", "low");
        assertThat(stores.find("A#crit", DivergenceType.PAID_BUT_CANCELLED)).contains(incident("crit", Severity.CRITICAL, 9_000_000, Incident.Status.OPEN));
        assertThat(stores.forPayment("A#done")).hasSize(1);

        DetectorState st = new DetectorState("A#x", Instant.parse("2026-10-11T10:00:00Z"),
                Map.of(DivergenceType.PAID_BUT_AWAITING, Instant.parse("2026-10-11T09:59:00Z")));
        stores.put(st);
        assertThat(stores.get("A#x")).contains(st);
        assertThat(stores.merchantCursor()).isEqualTo(MerchantOrders.Cursor.start());
        stores.saveMerchantCursor(new MerchantOrders.Cursor("2026-10-11T10:00:00.123456Z", "o_9"));
        assertThat(stores.merchantCursor()).isEqualTo(new MerchantOrders.Cursor("2026-10-11T10:00:00.123456Z", "o_9"));
        dynamo.close();
    }
}
