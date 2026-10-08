package dev.concord.ingest.local;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.InboundWebhook;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.IngestResult;
import dev.concord.ingest.IngestionService;
import dev.concord.ingest.Wiring;
import dev.concord.ingest.archive.InMemoryRawArchive;
import dev.concord.ingest.dedup.InMemoryDedupStore;
import dev.concord.ingest.queue.InMemoryFifoQueue;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * The same ingestion code behind a plain HTTP server, for local development without AWS.
 *
 * <p>{@code CONCORD_BACKEND=memory} (default) keeps everything in memory and exposes
 * {@code GET /local/stats}; {@code CONCORD_BACKEND=aws} uses real AWS or LocalStack.
 */
public final class LocalIngestServer implements AutoCloseable {

    private final HttpServer server;
    private final IngestionService service;
    private final Clock clock;
    private final Memory memory;

    /** In-memory backends, exposed so tests and demos can inspect what happened. */
    public record Memory(InMemoryRawArchive archive, InMemoryDedupStore dedup, InMemoryFifoQueue queue) {
    }

    public LocalIngestServer(int port, IngestionService service, Memory memory, Clock clock) throws IOException {
        this.service = service;
        this.memory = memory;
        this.clock = clock;
        // Backlog 0 means the JDK default of 50 pending connections; webhook bursts overflow it.
        this.server = HttpServer.create(new InetSocketAddress(port), 1024);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/webhooks/", this::webhook);
        server.createContext("/health", ex -> reply(ex, 200, "{\"status\":\"UP\"}"));
        server.createContext("/local/stats", this::stats);
        server.start();
    }

    public static LocalIngestServer inMemory(int port, IngestConfig config, Clock clock) throws IOException {
        Memory memory = new Memory(new InMemoryRawArchive(),
                new InMemoryDedupStore(config.dedupLease(), config.dedupTtl()), new InMemoryFifoQueue(clock));
        IngestionService service = Wiring.service(config, memory.archive(), memory.dedup(), memory.queue());
        return new LocalIngestServer(port, service, memory, clock);
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public Memory memory() {
        return memory;
    }

    private void webhook(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) {
            reply(ex, 405, "{\"error\":\"POST only\"}");
            return;
        }
        String[] parts = ex.getRequestURI().getPath().split("/");
        Optional<Gateway> gateway = parts.length == 3 ? Gateway.fromPath(parts[2]) : Optional.empty();
        byte[] body = ex.getRequestBody().readAllBytes();
        IngestResult result;
        if (gateway.isEmpty()) {
            result = IngestResult.UNKNOWN_GATEWAY;
        } else {
            Map<String, String> headers = new HashMap<>();
            ex.getRequestHeaders().forEach((name, values) -> headers.put(name, values.getFirst()));
            result = service.ingest(new InboundWebhook(gateway.get(), headers, body, clock.instant()));
        }
        reply(ex, result.httpStatus(), "{\"result\":\"" + result + "\"}");
    }

    private void stats(HttpExchange ex) throws IOException {
        if (memory == null) {
            reply(ex, 404, "{\"error\":\"stats only available with the memory backend\"}");
            return;
        }
        List<?> messages = memory.queue().messages();
        reply(ex, 200, String.format("{\"archived\":%d,\"dedupRecords\":%d,\"queueSendCalls\":%d,\"enqueued\":%d}",
                memory.archive().size(), memory.dedup().size(), memory.queue().sendCalls(), messages.size()));
    }

    private static void reply(HttpExchange ex, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }

    public static void main(String[] args) throws IOException {
        Map<String, String> env = System.getenv();
        IngestConfig config = IngestConfig.fromEnv(env);
        int port = Integer.parseInt(env.getOrDefault("PORT", "8080"));
        LocalIngestServer server = "aws".equalsIgnoreCase(env.getOrDefault("CONCORD_BACKEND", "memory"))
                ? new LocalIngestServer(port, Wiring.aws(config), null, Clock.systemUTC())
                : inMemory(port, config, Clock.systemUTC());
        System.out.println("concord ingestion listening on :" + server.port() + " for gateways " + config.secrets().keySet());
    }
}
