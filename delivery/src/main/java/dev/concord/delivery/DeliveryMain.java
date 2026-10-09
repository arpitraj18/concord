package dev.concord.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Container entry point: {@code PAYMENTS_STREAM_ARN}, {@code MERCHANT_EVENTS_URL},
 * {@code CONCORD_DELIVERY_SECRET}, {@code DELIVERY_DLQ_URL}.
 */
public final class DeliveryMain {

    private DeliveryMain() {
    }

    public static void main(String[] args) {
        var env = System.getenv();
        ObjectMapper json = new ObjectMapper();
        var http = UrlConnectionHttpClient.builder();
        DeliveryWorker worker = new DeliveryWorker(
                new DeliveryWorker.Settings(env.get("MERCHANT_EVENTS_URL"), 12, Duration.ofSeconds(1), Duration.ofMinutes(10),
                        Duration.ofSeconds(10)),
                new DeliverySigner(env.get("CONCORD_DELIVERY_SECRET")),
                new SqsDeadLetterSink(SqsClient.builder().httpClientBuilder(http).build(), env.get("DELIVERY_DLQ_URL"), json),
                json, Clock.systemUTC());
        DynamoDbStreamsOutboxReader reader = new DynamoDbStreamsOutboxReader(
                DynamoDbStreamsClient.builder().httpClientBuilder(http).build(), env.get("PAYMENTS_STREAM_ARN"), worker::submit);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            reader.stop();
            worker.close();
        }));
        reader.run();
    }
}
