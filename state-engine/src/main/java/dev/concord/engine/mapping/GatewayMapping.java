package dev.concord.engine.mapping;

import dev.concord.engine.machine.CanonicalState;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * How one gateway's webhooks translate into canonical states. Kept as versioned data, not code, so a
 * new gateway (or a new version of one) is a reviewed JSON file. In Phase 5 an LLM proposes these
 * files and a replay validator checks them before a human approves.
 *
 * @param eventType JSON pointer to the event type in the body
 * @param rules     first matching rule wins; an event matching no rule is recorded but changes nothing
 */
public record GatewayMapping(String gateway, int version, String eventType, List<Rule> rules, Fields fields) {

    public record Rule(String eventType, Condition when, CanonicalState state) {
    }

    /** Optional extra condition, e.g. only count a refund webhook whose refund_status is SUCCESS. */
    public record Condition(String pointer, String equals) {
    }

    public record Fields(Field amount, Field currency, Field orderId, Field occurredAt) {
    }

    /**
     * @param pointers candidates in order; the first one present in the body is used
     * @param unit     for amounts: MINOR (integer paise/cents) or MAJOR (decimal rupees/dollars)
     * @param format   for times: EPOCH_SECONDS, EPOCH_MILLIS or ISO_INSTANT
     */
    public record Field(List<String> pointers, String unit, String format) {
    }

    /** Rejects mappings that could never be right, before they are used on real money. */
    public void validate() {
        if (gateway == null || gateway.isBlank() || version < 1 || eventType == null || rules == null || rules.isEmpty()) {
            throw new InvalidMappingException("mapping needs gateway, version >= 1, eventType and at least one rule");
        }
        Set<String> unconditional = new HashSet<>();
        for (Rule r : rules) {
            if (r.eventType() == null || r.state() == null) {
                throw new InvalidMappingException("every rule needs eventType and state: " + r);
            }
            if (r.state() == CanonicalState.CREATED || r.state() == CanonicalState.UNKNOWN) {
                throw new InvalidMappingException(r.state() + " cannot be claimed by a webhook: " + r);
            }
            if (r.when() == null && !unconditional.add(r.eventType())) {
                throw new InvalidMappingException("two unconditional rules for " + r.eventType());
            }
        }
        if (fields != null && fields.amount() != null && !Set.of("MINOR", "MAJOR").contains(fields.amount().unit())) {
            throw new InvalidMappingException("amount.unit must be MINOR or MAJOR");
        }
        if (fields != null && fields.occurredAt() != null
                && !Set.of("EPOCH_SECONDS", "EPOCH_MILLIS", "ISO_INSTANT").contains(fields.occurredAt().format())) {
            throw new InvalidMappingException("occurredAt.format must be EPOCH_SECONDS, EPOCH_MILLIS or ISO_INSTANT");
        }
    }

    public String id() {
        return gateway + ".v" + version;
    }

    static Map<String, Integer> currencyExponents() {
        return Map.of("JPY", 0, "KRW", 0, "BHD", 3, "KWD", 3);
    }
}
