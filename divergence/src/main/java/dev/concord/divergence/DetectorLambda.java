package dev.concord.divergence;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.StateEngine;
import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.ingest.Gateway;
import dev.concord.resolver.merchant.HttpMerchantOrders;
import dev.concord.resolver.status.GatewayEndpoint;
import dev.concord.resolver.status.HttpGatewayClient;
import java.time.Clock;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Invoked by EventBridge Scheduler: reads the merchant change feed, runs due checks, then the windowed
 * sweep. Environment as for the resolver: PAYMENTS_TABLE, GATEWAY_&lt;X&gt;_STATUS_URL / _API_KEY,
 * MERCHANT_ORDERS_URL. (Outbox-triggered checks need a stream consumer; in AWS the sweep and the
 * merchant feed cover them within one schedule period.)
 */
public final class DetectorLambda implements RequestHandler<Map<String, Object>, Map<String, Integer>> {

    private static final class Holder {
        static final DivergenceDetector DETECTOR = build(System.getenv());
    }

    static DivergenceDetector build(Map<String, String> env) {
        ObjectMapper json = new ObjectMapper();
        Normalizer normalizer = Normalizer.withBundledMappings(json);
        DynamoDbClient dynamo = DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();
        String table = env.get("PAYMENTS_TABLE");
        Map<Gateway, GatewayEndpoint> endpoints = new EnumMap<>(Gateway.class);
        for (Gateway g : Gateway.values()) {
            String url = env.get("GATEWAY_" + g + "_STATUS_URL");
            if (url != null && !url.isBlank()) {
                endpoints.put(g, new GatewayEndpoint(url, env.get("GATEWAY_" + g + "_API_KEY")));
            }
        }
        HttpGatewayClient gateways = new HttpGatewayClient(endpoints, normalizer, json, Duration.ofSeconds(5),
                HttpGatewayClient.Limits.defaults());
        DynamoDbDivergenceStores stores = new DynamoDbDivergenceStores(dynamo, table);
        StateEngine engine = new StateEngine(normalizer, new DynamoDbPaymentStore(dynamo, table), Clock.systemUTC());
        return new DivergenceDetector(engine, gateways, new HttpMerchantOrders(env.get("MERCHANT_ORDERS_URL"), json),
                stores, stores, DetectorConfig.defaults(), Clock.systemUTC());
    }

    @Override
    public Map<String, Integer> handleRequest(Map<String, Object> event, Context context) {
        DivergenceDetector d = Holder.DETECTOR;
        int merchantChanges = d.pollMerchantChanges();
        int due = d.runDue();
        int swept = d.sweep();
        return Map.of("merchantChanges", merchantChanges, "dueChecks", due, "swept", swept);
    }
}
