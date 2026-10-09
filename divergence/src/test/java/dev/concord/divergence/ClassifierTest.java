package dev.concord.divergence;

import static dev.concord.divergence.DivergenceType.AMOUNT_MISMATCH;
import static dev.concord.divergence.DivergenceType.CONCORD_DRIFT;
import static dev.concord.divergence.DivergenceType.PAID_BUT_AWAITING;
import static dev.concord.divergence.DivergenceType.PAID_BUT_CANCELLED;
import static dev.concord.divergence.DivergenceType.REFUNDED_BUT_ACTIVE;
import static dev.concord.divergence.DivergenceType.UNPAID_BUT_FULFILLED;
import static org.assertj.core.api.Assertions.assertThat;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import dev.concord.resolver.merchant.MerchantOrder;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ClassifierTest {

    static Observation obs(String concord, String gateway, String order, long orderAmount) {
        PaymentRecord r = new PaymentRecord(Gateway.A, "p", CanonicalState.valueOf(concord), 3, 49_900L, "INR", "o",
                false, null, Instant.EPOCH, Instant.EPOCH);
        MerchantOrder o = order.equals("-") ? null : new MerchantOrder("o", "A", "p", order, orderAmount, "INR", null, null);
        return new Observation(r, gateway.equals("-") ? null : CanonicalState.valueOf(gateway), o);
    }

    @ParameterizedTest(name = "concord {0}, gateway {1}, order {2} -> {4}")
    @CsvSource({
            // concord    gateway     order              amount  expected
            "CAPTURED,   CAPTURED,   PAID,              49900,  ''",
            "CAPTURED,   CAPTURED,   CANCELLED,         49900,  PAID_BUT_CANCELLED",
            "SETTLED,    SETTLED,    PAYMENT_FAILED,    49900,  PAID_BUT_CANCELLED",
            "CAPTURED,   CAPTURED,   AWAITING_PAYMENT,  49900,  PAID_BUT_AWAITING",
            "FAILED,     FAILED,     PAID,              49900,  UNPAID_BUT_FULFILLED",
            "AUTHORIZED, AUTHORIZED, PAID,              49900,  UNPAID_BUT_FULFILLED",
            "CREATED,    -,          PAID,              49900,  UNPAID_BUT_FULFILLED",
            "REFUNDED,   REFUNDED,   PAID,              49900,  REFUNDED_BUT_ACTIVE",
            "REFUNDED,   REFUNDED,   REFUNDED,          49900,  ''",
            "FAILED,     FAILED,     CANCELLED,         49900,  ''",
            "AUTHORIZED, AUTHORIZED, AWAITING_PAYMENT,  49900,  ''",
            "CAPTURED,   CAPTURED,   PAID,              50000,  AMOUNT_MISMATCH",
            "CAPTURED,   REFUNDED,   PAID,              49900,  CONCORD_DRIFT REFUNDED_BUT_ACTIVE",
            "UNKNOWN,    PENDING,    AWAITING_PAYMENT,  49900,  ''",
            "CAPTURED,   -,          CANCELLED,         49900,  PAID_BUT_CANCELLED",
            "CAPTURED,   CAPTURED,   -,                 49900,  ''",
    })
    void classifies(String concord, String gateway, String order, long amount, String expected) {
        Set<DivergenceType> want = expected.isBlank() ? Set.of()
                : java.util.Arrays.stream(expected.trim().split(" ")).map(DivergenceType::valueOf).collect(java.util.stream.Collectors.toSet());
        assertThat(Classifier.classify(obs(concord.trim(), gateway.trim(), order.trim(), amount))).isEqualTo(want);
    }

    @org.junit.jupiter.api.Test
    void severityGrowsWithAmountAgeAndWeight() {
        long fresh = Severity.score(49_900, PAID_BUT_CANCELLED, java.time.Duration.ZERO);
        long hourOld = Severity.score(49_900, PAID_BUT_CANCELLED, java.time.Duration.ofHours(1));
        assertThat(hourOld).isEqualTo(2 * fresh);
        assertThat(Severity.score(49_900, PAID_BUT_AWAITING, java.time.Duration.ZERO)).isEqualTo(fresh / 3);
        assertThat(Severity.of(fresh)).isEqualTo(Severity.MEDIUM);
        assertThat(Severity.of(Severity.score(2_000_000, PAID_BUT_CANCELLED, java.time.Duration.ZERO))).isEqualTo(Severity.CRITICAL);
        assertThat(Severity.of(Severity.score(10_000, CONCORD_DRIFT, java.time.Duration.ZERO))).isEqualTo(Severity.LOW);
        assertThat(AMOUNT_MISMATCH.weight()).isEqualTo(REFUNDED_BUT_ACTIVE.weight());
        assertThat(UNPAID_BUT_FULFILLED.weight()).isEqualTo(PAID_BUT_CANCELLED.weight());
    }
}
