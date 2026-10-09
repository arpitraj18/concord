package dev.concord.merchant;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MerchantController {

    private final OrderService orders;
    private final ConcordSignature signature;
    private final ObjectMapper json;
    private final Clock clock = Clock.systemUTC();
    private final String concordRegisterUrl;
    private final java.util.Set<String> rejectEventsFor;
    private final java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();

    public MerchantController(OrderService orders, ConcordSignature signature, ObjectMapper json,
                              @org.springframework.beans.factory.annotation.Value("${merchant.concord-register-url:}") String concordRegisterUrl,
                              @org.springframework.beans.factory.annotation.Value("${merchant.reject-events-for-orders:}") String rejectEventsFor) {
        this.orders = orders;
        this.signature = signature;
        this.json = json;
        this.concordRegisterUrl = concordRegisterUrl;
        this.rejectEventsFor = java.util.concurrent.ConcurrentHashMap.newKeySet();
        java.util.Arrays.stream(rejectEventsFor.split(",")).map(String::trim).filter(x -> !x.isEmpty())
                .forEach(this.rejectEventsFor::add);
    }

    /** Concord's webhook into this merchant. 2xx means "never send this event again". */
    @PostMapping("/concord/events")
    public ResponseEntity<Map<String, String>> event(@RequestBody byte[] body,
            @RequestHeader(value = "Concord-Signature", required = false) String sig) throws IOException {
        if (!signature.valid(sig, body, clock.instant())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "bad signature"));
        }
        OrderService.ConcordEvent event = json.readValue(body, OrderService.ConcordEvent.class);
        if (event.orderId() != null && rejectEventsFor.contains(event.orderId())) {
            // fault injection: this merchant "loses" every event for these orders
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", "injected failure"));
        }
        return ResponseEntity.ok(Map.of("outcome", orders.apply(event).name()));
    }

    public record NewOrder(String orderId, String gateway, String paymentId, long amountMinor, String currency) {
    }

    @PostMapping("/orders")
    public ResponseEntity<Void> create(@RequestBody NewOrder o) throws IOException, InterruptedException {
        orders.create(o.orderId(), o.gateway(), o.paymentId(), o.amountMinor(), o.currency());
        if (!concordRegisterUrl.isBlank()) {
            // Tell Concord the payment exists, so it can be resolved even if every webhook is lost.
            http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(concordRegisterUrl))
                    .header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(json.writeValueAsString(o))).build(),
                    java.net.http.HttpResponse.BodyHandlers.discarding());
        }
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @PostMapping("/orders/{orderId}/cancel")
    public ResponseEntity<Map<String, Object>> cancel(@PathVariable String orderId) {
        return orders.cancel(orderId) ? ResponseEntity.ok(Map.of("cancelled", true))
                : ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("cancelled", false));
    }

    /** Fault injection at runtime: from now on, lose every Concord event for this order (503). */
    @PostMapping("/faults/reject-events/{orderId}")
    public Map<String, Object> rejectEvents(@PathVariable String orderId) {
        rejectEventsFor.add(orderId);
        return Map.of("rejecting", rejectEventsFor);
    }

    /** Simulates the redirect bug: the browser says "paid", the shop believes it and ships. */
    @PostMapping("/orders/{orderId}/mark-paid")
    public ResponseEntity<Map<String, Object>> markPaid(@PathVariable String orderId) {
        return orders.markPaidFromRedirect(orderId) ? ResponseEntity.ok(Map.of("paid", true))
                : ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("paid", false));
    }

    /**
     * Change feed for Concord's divergence detector: orders changed after the cursor, oldest first.
     * {@code next} is the cursor for the following page; an empty page means "caught up".
     */
    @GetMapping("/orders/changed")
    public Map<String, Object> changed(@org.springframework.web.bind.annotation.RequestParam(defaultValue = "1970-01-01T00:00:00Z") String since,
                                       @org.springframework.web.bind.annotation.RequestParam(defaultValue = "") String after,
                                       @org.springframework.web.bind.annotation.RequestParam(defaultValue = "200") int limit) {
        var page = orders.changedSince(since, after, Math.min(limit, 1000), java.time.Duration.ofSeconds(2));
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("orders", page);
        if (!page.isEmpty()) {
            var last = page.getLast();
            out.put("next", Map.of("since", last.get("updated_at"), "after", last.get("order_id")));
        }
        return out;
    }

    /** Used by Concord's merchant adapter (resolver: capture or release; Phase 4: divergence checks). */
    @GetMapping("/orders/{orderId}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable String orderId) {
        return orders.find(orderId).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return orders.stats();
    }
}
