package dev.concord.engine.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import dev.concord.engine.StateEngine;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.InMemoryPaymentStore;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.local.LocalIngestServer;
import dev.concord.ingest.queue.IngestedEvent;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The whole pipeline in one process, for local development without AWS: ingestion (in memory) plus a
 * consumer thread that drains its queue into the state engine (in memory).
 *
 * <pre>
 * POST /webhooks/{a|b|c}            ingestion
 * GET  /local/stats                 ingestion counters
 * GET  /local/payments/{gw}/{id}    canonical state + history
 * GET  /local/engine                engine counters
 * </pre>
 */
public final class LocalConcord {

    private LocalConcord() {
    }

    public static void main(String[] args) throws IOException {
        Map<String, String> env = System.getenv();
        IngestConfig config = IngestConfig.fromEnv(env);
        int port = Integer.parseInt(env.getOrDefault("PORT", "8080"));
        ObjectMapper json = new ObjectMapper().findAndRegisterModules()
                .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        LocalIngestServer server = LocalIngestServer.inMemory(port, config, Clock.systemUTC());
        InMemoryPaymentStore store = new InMemoryPaymentStore();
        StateEngine engine = new StateEngine(Normalizer.withBundledMappings(json), store, Clock.systemUTC());
        AtomicInteger consumed = new AtomicInteger();

        Thread.ofPlatform().daemon().name("local-engine").start(() -> {
            while (true) {
                List<IngestedEvent> batch = server.memory().queue().messagesFrom(consumed.get());
                for (IngestedEvent e : batch) {
                    try {
                        engine.process(e);
                    } catch (RuntimeException ex) {
                        System.err.println("engine failed on " + e.dedupKey() + ": " + ex);
                    }
                    consumed.incrementAndGet();
                }
                if (batch.isEmpty()) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        return;
                    }
                }
            }
        });

        server.addHandler("/local/engine", ex -> reply(ex, 200, json.writeValueAsString(Map.of(
                "consumed", consumed.get(), "payments", store.all().size(),
                "needsVerification", store.all().stream().filter(PaymentRecord::needsVerification).count()))));
        server.addHandler("/local/payments/", ex -> {
            String[] parts = ex.getRequestURI().getPath().split("/");
            Optional<Gateway> gateway = parts.length == 5 ? Gateway.fromPath(parts[3]) : Optional.empty();
            if (gateway.isEmpty()) {
                reply(ex, 404, "{\"error\":\"use /local/payments/{gateway}/{paymentId}\"}");
                return;
            }
            Optional<PaymentRecord> rec = store.find(gateway.get(), parts[4]);
            if (rec.isEmpty()) {
                reply(ex, 404, "{\"error\":\"unknown payment\"}");
                return;
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("payment", rec.get());
            body.put("history", store.history(gateway.get(), parts[4]));
            body.put("outbox", store.outbox(gateway.get(), parts[4]));
            reply(ex, 200, json.writeValueAsString(body));
        });
        System.out.println("concord (local, in memory) on :" + server.port() + " for gateways " + config.secrets().keySet());
    }

    private static void reply(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }
}
