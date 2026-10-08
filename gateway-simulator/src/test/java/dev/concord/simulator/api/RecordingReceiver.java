package dev.concord.simulator.api;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/** A tiny merchant endpoint for tests: records every webhook and answers with scripted status codes. */
final class RecordingReceiver implements AutoCloseable {

    record Received(Map<String, String> headers, String body) {
        String header(String name) {
            return headers.get(name.toLowerCase());
        }
    }

    private final HttpServer server;
    private final List<Received> received = new CopyOnWriteArrayList<>();
    private final Deque<Integer> scriptedStatuses = new ArrayDeque<>();

    RecordingReceiver() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext("/webhooks", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            Map<String, String> headers = exchange.getRequestHeaders().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().toLowerCase(), e -> e.getValue().getFirst()));
            received.add(new Received(headers, new String(body, StandardCharsets.UTF_8)));
            int status;
            synchronized (scriptedStatuses) {
                status = scriptedStatuses.isEmpty() ? 200 : scriptedStatuses.pollFirst();
            }
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/webhooks";
    }

    void respondWith(Integer... statuses) {
        synchronized (scriptedStatuses) {
            scriptedStatuses.addAll(List.of(statuses));
        }
    }

    List<Received> received() {
        return List.copyOf(received);
    }

    void reset() {
        received.clear();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
