package dev.concord.simulator.dialect;

import java.util.List;
import java.util.Locale;

public final class Dialects {

    private static final List<GatewayDialect> ALL = List.of(
            new GatewayADialect(), new GatewayBDialect(), new GatewayCDialect());

    private Dialects() {
    }

    public static GatewayDialect forId(String id) {
        String wanted = id.trim().toUpperCase(Locale.ROOT);
        return ALL.stream()
                .filter(d -> d.id().equals(wanted))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown gateway '" + id + "', expected A, B or C"));
    }

    public static List<GatewayDialect> all() {
        return ALL;
    }
}
