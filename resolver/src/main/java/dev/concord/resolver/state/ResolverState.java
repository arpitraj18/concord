package dev.concord.resolver.state;

import dev.concord.engine.machine.CanonicalState;
import java.time.Instant;

/**
 * The resolver's bookkeeping for one payment, stored apart from the payment itself
 * ({@code SK=RESOLVER}) so that scheduling a re-check never bumps the payment's version and never
 * conflicts with a webhook being applied.
 *
 * @param attempts               checks since the last progress (drives the backoff)
 * @param unresolvedSince        first check since the last progress (drives parking)
 * @param pendingCorrection      a backwards / other-branch state the status API reported once; applied
 *                               only if a second check, at least the confirmation delay later, agrees
 * @param pendingCorrectionVersion payment version the first observation was made against
 * @param parked                 gave up: needs a human (the dead-letter queue of the resolver)
 * @param actionTaken            capture or release already requested; never twice
 * @param confirmedVersion       payment version the status API last agreed with (0 = never)
 */
public record ResolverState(
        String paymentKey,
        int attempts,
        Instant unresolvedSince,
        Instant nextCheckAt,
        CanonicalState pendingCorrection,
        Instant pendingCorrectionSince,
        long pendingCorrectionVersion,
        boolean parked,
        String parkedReason,
        boolean actionTaken,
        Instant lastCheckedAt,
        String lastResult,
        long confirmedVersion) {

    public static ResolverState fresh(String paymentKey) {
        return new ResolverState(paymentKey, 0, null, null, null, null, 0, false, null, false, null, null, 0);
    }

    public ResolverState checked(Instant now, String result) {
        return new ResolverState(paymentKey, attempts, unresolvedSince == null ? now : unresolvedSince, nextCheckAt, pendingCorrection, pendingCorrectionSince,
                pendingCorrectionVersion, parked, parkedReason, actionTaken, now, result, confirmedVersion);
    }

    public ResolverState retryAt(Instant at, int newAttempts) {
        return new ResolverState(paymentKey, newAttempts, unresolvedSince, at, pendingCorrection, pendingCorrectionSince,
                pendingCorrectionVersion, parked, parkedReason, actionTaken, lastCheckedAt, lastResult, confirmedVersion);
    }

    public ResolverState progressed() {
        return new ResolverState(paymentKey, 0, null, null, null, null, 0, false, null, actionTaken, lastCheckedAt, lastResult, confirmedVersion);
    }

    public ResolverState suspectCorrection(CanonicalState state, Instant since, long version, Instant recheckAt) {
        return new ResolverState(paymentKey, attempts, unresolvedSince, recheckAt, state, since, version, parked, parkedReason,
                actionTaken, lastCheckedAt, lastResult, confirmedVersion);
    }

    public ResolverState park(String reason) {
        return new ResolverState(paymentKey, attempts, unresolvedSince, null, pendingCorrection, pendingCorrectionSince,
                pendingCorrectionVersion, true, reason, actionTaken, lastCheckedAt, lastResult, confirmedVersion);
    }

    public ResolverState confirmed(long version) {
        return new ResolverState(paymentKey, attempts, unresolvedSince, nextCheckAt, pendingCorrection, pendingCorrectionSince,
                pendingCorrectionVersion, parked, parkedReason, actionTaken, lastCheckedAt, lastResult, version);
    }

    public ResolverState withAction() {
        return new ResolverState(paymentKey, attempts, unresolvedSince, nextCheckAt, pendingCorrection, pendingCorrectionSince,
                pendingCorrectionVersion, parked, parkedReason, true, lastCheckedAt, lastResult, confirmedVersion);
    }
}
