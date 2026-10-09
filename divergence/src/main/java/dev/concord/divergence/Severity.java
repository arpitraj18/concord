package dev.concord.divergence;

import java.time.Duration;

/**
 * Severity = amount x age x type weight. A large divergence matters more, an old one more than a new
 * one (the customer has been waiting, the chargeback window is closing), and losing goods or charging
 * for nothing more than a merchant that is merely behind.
 *
 * <p>score = amountMinor x weight x (1 + ageMinutes / 60): it doubles after an hour.
 */
public enum Severity {
    LOW, MEDIUM, HIGH, CRITICAL;

    static final long MEDIUM_FROM = 50_000;       // e.g. Rs 500 x weight 1, fresh
    static final long HIGH_FROM = 500_000;        // e.g. Rs 1,700 paid-but-cancelled, fresh
    static final long CRITICAL_FROM = 5_000_000;  // e.g. Rs 17,000 paid-but-cancelled, fresh

    public static long score(long amountMinor, DivergenceType type, Duration age) {
        double ageFactor = 1.0 + Math.max(0, age.toSeconds()) / 3600.0;
        return Math.round(amountMinor * type.weight() * ageFactor);
    }

    public static Severity of(long score) {
        if (score >= CRITICAL_FROM) {
            return CRITICAL;
        }
        if (score >= HIGH_FROM) {
            return HIGH;
        }
        return score >= MEDIUM_FROM ? MEDIUM : LOW;
    }
}
