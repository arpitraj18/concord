package dev.concord.engine.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.StateEngine;
import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.mapping.Normalizer;
import java.time.Clock;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Container entry point: {@code QUEUE_URL}, {@code PAYMENTS_TABLE}, {@code WORKER_THREADS} (default 4).
 * Region, credentials and optional {@code AWS_ENDPOINT_URL} come from the standard AWS SDK chain.
 *
 * <p>Several threads poll in parallel; SQS FIFO never hands out two in-flight messages of the same
 * group, so a payment's events are still processed one at a time and in order.
 */
public final class WorkerMain {

    private WorkerMain() {
    }

    public static void main(String[] args) {
        String queueUrl = required("QUEUE_URL");
        String table = required("PAYMENTS_TABLE");
        int threads = Integer.parseInt(System.getenv().getOrDefault("WORKER_THREADS", "4"));
        ObjectMapper json = new ObjectMapper();
        var http = UrlConnectionHttpClient.builder();
        SqsClient sqs = SqsClient.builder().httpClientBuilder(http).build();
        DynamoDbClient dynamo = DynamoDbClient.builder().httpClientBuilder(http).build();
        StateEngine engine = new StateEngine(Normalizer.withBundledMappings(json),
                new DynamoDbPaymentStore(dynamo, table), Clock.systemUTC());
        for (int i = 0; i < threads; i++) {
            SqsStateWorker worker = new SqsStateWorker(sqs, queueUrl, engine, json);
            Runtime.getRuntime().addShutdownHook(new Thread(worker::stop));
            Thread.ofPlatform().name("state-worker-" + i).start(worker);
        }
        System.out.println("concord state worker: " + threads + " threads on " + queueUrl);
    }

    private static String required(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException(name + " must be set");
        }
        return v;
    }
}
