package dev.concord.ingest.aws;

import java.net.URI;
import java.util.Map;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/** ElasticMQ in the test JVM: speaks the SQS API, including FIFO ordering and deduplication. */
public final class ElasticMq implements AutoCloseable {

    private final SQSRestServer server;
    private final SqsClient client;

    private ElasticMq(SQSRestServer server, SqsClient client) {
        this.server = server;
        this.client = client;
    }

    public static ElasticMq start() {
        SQSRestServer server = SQSRestServerBuilder.withDynamicPort().withInterface("127.0.0.1").start();
        int port = server.waitUntilStarted().localAddress().getPort();
        SqsClient client = SqsClient.builder()
                .endpointOverride(URI.create("http://127.0.0.1:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
        return new ElasticMq(server, client);
    }

    public SqsClient client() {
        return client;
    }

    public String createFifoQueue(String name) {
        return client.createQueue(b -> b.queueName(name).attributesWithStrings(Map.of("FifoQueue", "true"))).queueUrl();
    }

    @Override
    public void close() {
        client.close();
        server.stopAndWait();
    }
}
