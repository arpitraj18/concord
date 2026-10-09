package dev.concord.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import dev.concord.delivery.DeliveryWorker;
import dev.concord.divergence.Incident;
import dev.concord.divergence.InMemoryIncidentStore;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.store.InMemoryPaymentStore;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import dev.concord.resolver.state.InMemoryResolverStateStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** The dashboard's JSON API and static files. Read-only. */
final class Api {

    private final ObjectMapper json;
    private final InMemoryPaymentStore store;
    private final InMemoryIncidentStore incidents;
    private final InMemoryResolverStateStore resolverStates;
    private final Map<String, Integer> resolverTotals;
    private final Map<String, Integer> detectorTotals;
    private final AtomicInteger consumed;
    private final DeliveryWorker delivery;
    private final List<String> deadLetters;

    Api(ObjectMapper json, InMemoryPaymentStore store, InMemoryIncidentStore incidents, InMemoryResolverStateStore resolverStates,
        Map<String, Integer> resolverTotals, Map<String, Integer> detectorTotals, AtomicInteger consumed,
        DeliveryWorker delivery, List<String> deadLetters) {
        this.json = json;
        this.store = store;
        this.incidents = incidents;
        this.resolverStates = resolverStates;
        this.resolverTotals = resolverTotals;
        this.detectorTotals = detectorTotals;
        this.consumed = consumed;
        this.delivery = delivery;
        this.deadLetters = deadLetters;
    }

    void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String query = ex.getRequestURI().getQuery() == null ? "" : ex.getRequestURI().getQuery();
        Object body;
        if (path.equals("/api/summary")) {
            body = summary();
        } else if (path.equals("/api/incidents")) {
            body = query.contains("status=all") ? incidents.all().stream()
                    .sorted(Comparator.comparing(Incident::updatedAt).reversed()).toList() : incidents.open(500);
        } else if (path.equals("/api/payments")) {
            String state = param(query, "state");
            body = store.all().stream()
                    .filter(r -> state == null || r.state().name().equals(state))
                    .sorted(Comparator.comparing(PaymentRecord::updatedAt).reversed()).limit(200).toList();
        } else if (path.startsWith("/api/payments/")) {
            String[] parts = path.split("/");
            Optional<Gateway> g = parts.length == 5 ? Gateway.fromPath(parts[3]) : Optional.empty();
            Optional<PaymentRecord> rec = g.flatMap(gw -> store.find(gw, parts[4]));
            if (rec.isEmpty()) {
                LocalConcord.reply(ex, 404, "{\"error\":\"unknown payment\"}");
                return;
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("payment", rec.get());
            m.put("history", store.history(g.get(), parts[4]));
            m.put("outbox", store.outbox(g.get(), parts[4]));
            m.put("incidents", incidents.forPayment(rec.get().key()));
            m.put("resolver", resolverStates.get(rec.get().key()).orElse(null));
            body = m;
        } else {
            LocalConcord.reply(ex, 404, "{\"error\":\"not found\"}");
            return;
        }
        LocalConcord.reply(ex, 200, json.writeValueAsString(body));
    }

    private Map<String, Object> summary() {
        List<PaymentRecord> all = store.all();
        Map<CanonicalState, Long> byState = new EnumMap<>(CanonicalState.class);
        for (CanonicalState s : CanonicalState.values()) {
            byState.put(s, 0L);
        }
        all.forEach(r -> byState.merge(r.state(), 1L, Long::sum));
        Map<String, Long> openBySeverity = new LinkedHashMap<>();
        for (var sev : dev.concord.divergence.Severity.values()) {
            openBySeverity.put(sev.name(), incidents.open(10_000).stream().filter(i -> i.severity() == sev).count());
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("payments", all.size());
        m.put("byState", byState);
        m.put("flagged", all.stream().filter(PaymentRecord::needsVerification).count());
        m.put("parked", resolverStates.parked().size());
        m.put("openIncidents", incidents.open(10_000).size());
        m.put("openBySeverity", openBySeverity);
        m.put("eventsProcessed", consumed.get());
        m.put("resolver", resolverTotals);
        m.put("detector", detectorTotals);
        m.put("delivery", delivery == null ? Map.of() : Map.of("delivered", delivery.delivered(), "attempts", delivery.attempts(),
                "pending", delivery.pending(), "deadLettered", deadLetters.size()));
        return m;
    }

    private static String param(String query, String name) {
        for (String kv : query.split("&")) {
            String[] p = kv.split("=", 2);
            if (p.length == 2 && p[0].equals(name) && !p[1].isBlank()) {
                return p[1];
            }
        }
        return null;
    }

    private static final Map<String, String> TYPES = Map.of(
            "html", "text/html; charset=utf-8", "js", "text/javascript; charset=utf-8", "css", "text/css; charset=utf-8");

    /** Serves /dashboard/* from the classpath; "/" is the dashboard itself. */
    static void serveDashboard(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String file = path.equals("/") ? "index.html" : path.startsWith("/dashboard/") ? path.substring(11) : null;
        if (file == null || file.contains("..")) {
            LocalConcord.reply(ex, 404, "{\"error\":\"not found\"}");
            return;
        }
        try (InputStream in = Api.class.getResourceAsStream("/dashboard/" + file)) {
            if (in == null) {
                LocalConcord.reply(ex, 404, "{\"error\":\"not found\"}");
                return;
            }
            byte[] bytes = in.readAllBytes();
            String ext = file.substring(file.lastIndexOf('.') + 1);
            ex.getResponseHeaders().set("Content-Type", TYPES.getOrDefault(ext, "application/octet-stream"));
            ex.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = ex.getResponseBody()) {
                out.write(bytes);
            }
        }
    }
}
