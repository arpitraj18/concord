package dev.concord.engine.replay;

import dev.concord.engine.store.PaymentRecord;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Compares two sets of payment states (e.g. live vs replayed).
 *
 * <p>Versions are not compared: when a webhook in the middle is missing or late, the same final state
 * can be reached in one jump or in two steps, so the count of changes depends on arrival order while
 * the state does not. What must match is what the payment <em>is</em>.
 *
 * <p>Flagged payments (the gateway contradicted itself) are compared on the flag only: which of the two
 * contradicting claims is held until verification depends on which arrived first, and the resolver
 * settles it from the status API (Phase 3). See docs/design/02-state-machine.md.
 */
public final class StateComparison {

    public record Result(int compared, List<String> mismatches, List<String> flaggedHoldingDifferentClaim,
                         Set<String> onlyInLeft, Set<String> onlyInRight) {

        public boolean identical() {
            return mismatches.isEmpty() && onlyInLeft.isEmpty() && onlyInRight.isEmpty();
        }
    }

    private StateComparison() {
    }

    public static Result compare(List<PaymentRecord> left, List<PaymentRecord> right) {
        Map<String, PaymentRecord> l = left.stream().collect(Collectors.toMap(PaymentRecord::key, Function.identity()));
        Map<String, PaymentRecord> r = right.stream().collect(Collectors.toMap(PaymentRecord::key, Function.identity()));
        List<String> mismatches = new ArrayList<>();
        List<String> flaggedDiffering = new ArrayList<>();
        Set<String> onlyLeft = new HashSet<>(l.keySet());
        onlyLeft.removeAll(r.keySet());
        Set<String> onlyRight = new HashSet<>(r.keySet());
        onlyRight.removeAll(l.keySet());

        for (Map.Entry<String, PaymentRecord> e : l.entrySet()) {
            PaymentRecord a = e.getValue();
            PaymentRecord b = r.get(e.getKey());
            if (b == null) {
                continue;
            }
            if (a.needsVerification() != b.needsVerification()) {
                mismatches.add(e.getKey() + ": flag " + a.needsVerification() + " vs " + b.needsVerification());
                continue;
            }
            if (!Objects.equals(a.amountMinor(), b.amountMinor()) || !Objects.equals(a.currency(), b.currency())) {
                mismatches.add(e.getKey() + ": amount " + a.amountMinor() + a.currency() + " vs " + b.amountMinor() + b.currency());
                continue;
            }
            if (a.state() != b.state()) {
                if (a.needsVerification()) {
                    flaggedDiffering.add(e.getKey());
                } else {
                    mismatches.add(e.getKey() + ": state " + a.state() + " vs " + b.state());
                }
            }
        }
        return new Result(l.size(), mismatches, flaggedDiffering, onlyLeft, onlyRight);
    }
}
