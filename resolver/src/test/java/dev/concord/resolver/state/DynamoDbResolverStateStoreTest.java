package dev.concord.resolver.state;

import static org.assertj.core.api.Assertions.assertThat;

import dev.concord.engine.aws.PaymentsTable;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.aws.DynamoDbLocal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DynamoDbResolverStateStoreTest {

    @Test
    void roundTripsAndFindsParkedPayments() {
        var dynamo = DynamoDbLocal.client();
        PaymentsTable.create(dynamo, "resolver-state-test");
        DynamoDbResolverStateStore store = new DynamoDbResolverStateStore(dynamo, "resolver-state-test");
        Instant t = Instant.parse("2026-10-09T10:00:00Z");

        ResolverState s = ResolverState.fresh("A#p1").checked(t, "CAPTURED").retryAt(t.plusSeconds(30), 2)
                .suspectCorrection(CanonicalState.AUTHORIZED, t, 4, t.plusSeconds(30)).withAction();
        store.put(s);
        assertThat(store.get("A#p1")).contains(s);
        assertThat(store.parked()).isEmpty();

        ResolverState parked = ResolverState.fresh("A#p2").checked(t, "no answer").park("gave up");
        store.put(parked);
        assertThat(store.parked()).containsExactly(parked);
        assertThat(store.get("A#nobody")).isEmpty();
        dynamo.close();
    }
}
