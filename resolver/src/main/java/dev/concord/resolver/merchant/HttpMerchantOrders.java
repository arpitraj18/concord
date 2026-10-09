package dev.concord.resolver.merchant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** The sample merchant's adapter: {@code GET /orders/{id}} and {@code GET /orders/changed}. */
public final class HttpMerchantOrders implements MerchantOrders {

    private final String baseUrl;
    private final ObjectMapper json;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    public HttpMerchantOrders(String baseUrl, ObjectMapper json) {
        this.baseUrl = baseUrl;
        this.json = json;
    }

    @Override
    public Optional<MerchantOrder> getOrder(String orderId) {
        JsonNode body = get("/orders/" + URLEncoder.encode(orderId, StandardCharsets.UTF_8));
        return body == null ? Optional.empty() : Optional.of(toOrder(body));
    }

    @Override
    public Page changedSince(Cursor cursor, int limit) {
        JsonNode body = get("/orders/changed?since=" + URLEncoder.encode(cursor.since(), StandardCharsets.UTF_8)
                + "&after=" + URLEncoder.encode(cursor.after(), StandardCharsets.UTF_8) + "&limit=" + limit);
        if (body == null) {
            return new Page(List.of(), cursor);
        }
        List<MerchantOrder> orders = new ArrayList<>();
        body.path("orders").forEach(o -> orders.add(toOrder(o)));
        JsonNode next = body.path("next");
        return new Page(orders, next.isMissingNode() ? cursor : new Cursor(next.get("since").asText(), next.get("after").asText()));
    }

    private static MerchantOrder toOrder(JsonNode o) {
        return new MerchantOrder(o.path("order_id").asText(), o.path("gateway").asText(null), o.path("payment_id").asText(null),
                o.path("status").asText(), o.hasNonNull("amount_minor") ? o.get("amount_minor").asLong() : null,
                o.path("currency").asText(null), o.hasNonNull("payment_state") ? o.get("payment_state").asText() : null,
                o.hasNonNull("updated_at") ? Instant.parse(o.get("updated_at").asText()) : null);
    }

    private JsonNode get(String path) {
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .timeout(Duration.ofSeconds(3)).build(), HttpResponse.BodyHandlers.ofString());
            return r.statusCode() == 200 ? json.readTree(r.body()) : null;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }
}
