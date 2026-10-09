package dev.concord.resolver.merchant;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Calls {@code GET <baseUrl>/orders/{orderId}} and reads its {@code status}. */
public final class HttpMerchantOrders implements MerchantOrders {

    private final String baseUrl;
    private final ObjectMapper json;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    public HttpMerchantOrders(String baseUrl, ObjectMapper json) {
        this.baseUrl = baseUrl;
        this.json = json;
    }

    @Override
    public OrderStatus orderStatus(String orderId) {
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/orders/" + orderId))
                    .timeout(Duration.ofSeconds(3)).build(), HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() != 200) {
                return OrderStatus.UNKNOWN;
            }
            String status = json.readTree(r.body()).path("status").asText("");
            return status.equals("CANCELLED") ? OrderStatus.CANCELLED : OrderStatus.VALID;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return OrderStatus.UNKNOWN;
        }
    }
}
