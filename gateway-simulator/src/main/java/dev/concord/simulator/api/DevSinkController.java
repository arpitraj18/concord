package dev.concord.simulator.api;

import dev.concord.simulator.config.SimulatorProperties;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * A throwaway webhook receiver so the simulator can be demoed before Concord's ingestion exists.
 * Point a scenario's targetUrl at {@code /dev/sink} and read what arrived from {@code GET /dev/sink}.
 */
@RestController
@RequestMapping("/dev/sink")
@ConditionalOnProperty(name = "simulator.dev-sink.enabled", havingValue = "true", matchIfMissing = true)
public class DevSinkController {

    private final ConcurrentLinkedDeque<Received> received = new ConcurrentLinkedDeque<>();
    private final int capacity;
    private final Clock clock;

    public DevSinkController(SimulatorProperties props, Clock clock) {
        this.capacity = props.devSink().capacity();
        this.clock = clock;
    }

    @PostMapping({"", "/**"})
    public Map<String, String> receive(HttpServletRequest request, @RequestBody byte[] body) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (String name : Collections.list(request.getHeaderNames())) {
            String lower = name.toLowerCase(Locale.ROOT);
            if (lower.startsWith("x-") || lower.contains("signature")) {
                headers.put(name, request.getHeader(name));
            }
        }
        received.addLast(new Received(clock.instant(), request.getRequestURI(), headers,
                new String(body, StandardCharsets.UTF_8)));
        while (received.size() > capacity) {
            received.pollFirst();
        }
        return Map.of("status", "ok");
    }

    @GetMapping
    public List<Received> list() {
        return new ArrayList<>(received);
    }

    @DeleteMapping
    public void clear() {
        received.clear();
    }

    public record Received(Instant receivedAt, String path, Map<String, String> headers, String body) {
    }
}
