package dev.concord.resolver.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.ingest.Gateway;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * Calls each gateway's status API over HTTP, with one circuit breaker and one rate limiter <em>per
 * gateway</em>, so one gateway's outage or rate limit never slows the others.
 *
 * <ul>
 *   <li>Rate limiter: never exceed the gateway's API quota. A call that gets no permit is not queued:
 *       it returns Unavailable and the resolver reschedules it with backoff.</li>
 *   <li>Circuit breaker: after half the recent calls fail, stop calling for a while instead of
 *       hammering a gateway that is down. 404 is an answer, not a failure.</li>
 * </ul>
 */
public final class HttpGatewayClient implements StatusClient, GatewayActions {

    private final Map<Gateway, GatewayEndpoint> endpoints;
    private final Normalizer normalizer;
    private final ObjectMapper json;
    private final HttpClient http;
    private final Duration timeout;
    private final Map<Gateway, CircuitBreaker> breakers = new EnumMap<>(Gateway.class);
    private final Map<Gateway, RateLimiter> limiters = new EnumMap<>(Gateway.class);

    public record Limits(int requestsPerSecond, float failureRateThreshold, int slidingWindow, Duration openFor) {
        public static Limits defaults() {
            return new Limits(50, 50f, 20, Duration.ofSeconds(10));
        }
    }

    public HttpGatewayClient(Map<Gateway, GatewayEndpoint> endpoints, Normalizer normalizer, ObjectMapper json,
                             Duration timeout, Limits limits) {
        this.endpoints = Map.copyOf(endpoints);
        this.normalizer = normalizer;
        this.json = json;
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
        for (Gateway g : endpoints.keySet()) {
            breakers.put(g, CircuitBreaker.of("status-" + g, CircuitBreakerConfig.custom()
                    .failureRateThreshold(limits.failureRateThreshold())
                    .slidingWindowSize(limits.slidingWindow())
                    .minimumNumberOfCalls(Math.min(10, limits.slidingWindow()))
                    .waitDurationInOpenState(limits.openFor())
                    .permittedNumberOfCallsInHalfOpenState(3)
                    .recordException(e -> e instanceof GatewayDown)
                    .build()));
            limiters.put(g, RateLimiter.of("status-" + g, RateLimiterConfig.custom()
                    .limitForPeriod(limits.requestsPerSecond())
                    .limitRefreshPeriod(Duration.ofSeconds(1))
                    .timeoutDuration(Duration.ZERO)
                    .build()));
        }
    }

    public CircuitBreaker.State circuitState(Gateway gateway) {
        return breakers.get(gateway).getState();
    }

    @Override
    public StatusResult fetch(Gateway gateway, String paymentId) {
        Optional<HttpResponse<String>> response = call(gateway, "GET", "/v1/payments/" + paymentId);
        if (response.isEmpty()) {
            return new StatusResult.Unavailable(lastReason.get());
        }
        HttpResponse<String> r = response.get();
        if (r.statusCode() == 404) {
            return new StatusResult.NotFound();
        }
        try {
            JsonNode body = json.readTree(r.body());
            Optional<CanonicalState> state = normalizer.statusState(gateway, body);
            String raw = body.toString();
            return state.<StatusResult>map(s -> new StatusResult.Found(s, raw))
                    .orElseGet(() -> new StatusResult.Unavailable("unmapped status " + raw));
        } catch (IOException e) {
            return new StatusResult.Unavailable("unreadable status response");
        }
    }

    @Override
    public CanonicalState capture(Gateway gateway, String paymentId) {
        return action(gateway, paymentId, "capture");
    }

    @Override
    public CanonicalState release(Gateway gateway, String paymentId) {
        return action(gateway, paymentId, "void");
    }

    private CanonicalState action(Gateway gateway, String paymentId, String verb) {
        Optional<HttpResponse<String>> response = call(gateway, "POST", "/v1/payments/" + paymentId + "/" + verb);
        if (response.isEmpty() || response.get().statusCode() != 200) {
            return null;
        }
        try {
            return normalizer.statusState(gateway, json.readTree(response.get().body())).orElse(null);
        } catch (IOException e) {
            return null;
        }
    }

    private final ThreadLocal<String> lastReason = ThreadLocal.withInitial(() -> "unknown");

    /** Empty means "no usable answer"; the reason is left in {@link #lastReason}. */
    private Optional<HttpResponse<String>> call(Gateway gateway, String method, String path) {
        GatewayEndpoint endpoint = endpoints.get(gateway);
        if (endpoint == null) {
            lastReason.set("no endpoint configured for gateway " + gateway);
            return Optional.empty();
        }
        if (!limiters.get(gateway).acquirePermission()) {
            lastReason.set("rate limited (client side)");
            return Optional.empty();
        }
        try {
            return Optional.of(breakers.get(gateway).executeCallable(() -> {
                HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(endpoint.baseUrl() + path)).timeout(timeout);
                if (endpoint.apiKey() != null && !endpoint.apiKey().isBlank()) {
                    req.header("Authorization", "Bearer " + endpoint.apiKey());
                }
                req.method(method, HttpRequest.BodyPublishers.noBody());
                HttpResponse<String> r = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() >= 500 || r.statusCode() == 429) {
                    throw new GatewayDown("HTTP " + r.statusCode());
                }
                return r;
            }));
        } catch (CallNotPermittedException e) {
            lastReason.set("circuit open for gateway " + gateway);
        } catch (GatewayDown e) {
            lastReason.set(e.getMessage());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            lastReason.set(e.getClass().getSimpleName());
        }
        return Optional.empty();
    }

    /** A failure that counts against the circuit breaker: 5xx, 429, timeouts, connection errors. */
    static final class GatewayDown extends IOException {
        GatewayDown(String message) {
            super(message);
        }
    }
}
