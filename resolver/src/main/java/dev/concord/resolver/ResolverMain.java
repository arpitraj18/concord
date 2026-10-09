package dev.concord.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.StateEngine;
import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.ingest.Gateway;
import dev.concord.resolver.merchant.HttpMerchantOrders;
import dev.concord.resolver.state.DynamoDbResolverStateStore;
import dev.concord.resolver.status.GatewayEndpoint;
import dev.concord.resolver.status.HttpGatewayClient;
import java.time.Clock;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Builds the resolver from environment variables:
 * {@code PAYMENTS_TABLE}, {@code GATEWAY_<X>_STATUS_URL}, {@code GATEWAY_<X>_API_KEY},
 * {@code MERCHANT_ORDERS_URL}, {@code SWEEP_INTERVAL_SECONDS} (default 30).
 *
 * <p>In AWS, EventBridge Scheduler invokes {@link ResolverLambda} every minute instead of this loop.
 */
public final class ResolverMain {

    private ResolverMain() {
    }

    public static Resolver fromEnv(Map<String, String> env) {
        ObjectMapper json = new ObjectMapper();
        Normalizer normalizer = Normalizer.withBundledMappings(json);
        DynamoDbClient dynamo = DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();
        String table = require(env, "PAYMENTS_TABLE");
        Map<Gateway, GatewayEndpoint> endpoints = new EnumMap<>(Gateway.class);
        for (Gateway g : Gateway.values()) {
            String url = env.get("GATEWAY_" + g + "_STATUS_URL");
            if (url != null && !url.isBlank()) {
                endpoints.put(g, new GatewayEndpoint(url, env.get("GATEWAY_" + g + "_API_KEY")));
            }
        }
        HttpGatewayClient gateways = new HttpGatewayClient(endpoints, normalizer, json, Duration.ofSeconds(5),
                HttpGatewayClient.Limits.defaults());
        StateEngine engine = new StateEngine(normalizer, new DynamoDbPaymentStore(dynamo, table), Clock.systemUTC());
        return new Resolver(engine, normalizer, gateways, gateways,
                new HttpMerchantOrders(require(env, "MERCHANT_ORDERS_URL"), json),
                new DynamoDbResolverStateStore(dynamo, table), ResolverConfig.defaults(), Clock.systemUTC());
    }

    static String require(Map<String, String> env, String name) {
        String v = env.get(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException(name + " must be set");
        }
        return v;
    }

    public static void main(String[] args) throws InterruptedException {
        Resolver resolver = fromEnv(System.getenv());
        long interval = Long.parseLong(System.getenv().getOrDefault("SWEEP_INTERVAL_SECONDS", "30"));
        while (true) {
            try {
                System.out.println("sweep: " + resolver.sweep());
            } catch (RuntimeException e) {
                System.err.println("sweep failed: " + e);
            }
            Thread.sleep(interval * 1000);
        }
    }
}
