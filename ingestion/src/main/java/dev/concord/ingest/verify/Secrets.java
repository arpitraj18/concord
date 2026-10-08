package dev.concord.ingest.verify;

import java.util.List;

/**
 * The secrets a gateway's signatures may be made with. More than one is allowed so a secret can be
 * rotated without downtime: add the new one, switch the gateway, then remove the old one.
 */
public record Secrets(List<String> values) {

    public Secrets {
        values = values.stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
        if (values.isEmpty()) {
            throw new IllegalArgumentException("at least one secret is required");
        }
    }

    public static Secrets of(String... values) {
        return new Secrets(List.of(values));
    }

    /** Parses a comma-separated list, e.g. from an environment variable. */
    public static Secrets parse(String csv) {
        return new Secrets(csv == null ? List.of() : List.of(csv.split(",")));
    }
}
