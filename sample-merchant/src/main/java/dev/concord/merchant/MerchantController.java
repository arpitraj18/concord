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
    private final java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();

    public MerchantController(OrderService orders, ConcordSignature signature, ObjectMapper json,
                              @org.springframework.beans.factory.annotation.Value("${merchant.concord-register-url:}") String concordRegisterUrl) {
        this.orders = orders;
        this.signature = signature;
        this.json = json;
        this.concordRegisterUrl = concordRegisterUrl;
    }

    /** Concord's webhook into this merchant. 2xx means "never send this event again". */
    @PostMapping("/concord/events")
    public ResponseEntity<Map<String, String>> event(@RequestBody byte[] body,
            @RequestHeader(value = "Concord-Signature", required = false) String sig) throws IOException {
        if (!signature.valid(sig, body, clock.instant())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "bad signature"));
        }
        OrderService.ConcordEvent event = json.readValue(body, OrderService.ConcordEvent.class);
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
