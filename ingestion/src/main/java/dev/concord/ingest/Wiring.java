package dev.concord.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.ingest.archive.RawArchive;
import dev.concord.ingest.aws.DynamoDbDedupStore;
import dev.concord.ingest.aws.S3RawArchive;
import dev.concord.ingest.aws.SqsEventQueue;
import dev.concord.ingest.dedup.DedupStore;
import dev.concord.ingest.envelope.EnvelopeExtractor;
import dev.concord.ingest.queue.EventQueue;
import dev.concord.ingest.verify.GatewayAVerifier;
import dev.concord.ingest.verify.GatewayBVerifier;
import dev.concord.ingest.verify.GatewayCVerifier;
import dev.concord.ingest.verify.SignatureVerifier;
import java.util.EnumMap;
import java.util.Map;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Plain constructor wiring. No DI framework: there are five objects and Lambda cold starts matter. */
public final class Wiring {

    public static final ObjectMapper JSON = new ObjectMapper();

    private Wiring() {
    }

    public static Map<Gateway, SignatureVerifier> verifiers(IngestConfig config) {
        Map<Gateway, SignatureVerifier> verifiers = new EnumMap<>(Gateway.class);
        config.secrets().forEach((gateway, secrets) -> verifiers.put(gateway, switch (gateway) {
            case A -> new GatewayAVerifier(secrets);
            case B -> new GatewayBVerifier(secrets, config.signatureTolerance());
            case C -> new GatewayCVerifier(secrets, config.signatureTolerance());
        }));
        return verifiers;
    }

    public static IngestionService service(IngestConfig config, RawArchive archive, DedupStore dedup, EventQueue queue) {
        return new IngestionService(verifiers(config), new EnvelopeExtractor(JSON), archive, dedup, queue, JSON);
    }

    /** Real AWS (or LocalStack via AWS_ENDPOINT_URL). Region and credentials come from the standard SDK chain. */
    public static IngestionService aws(IngestConfig config) {
        config.requireAwsSettings();
        var http = UrlConnectionHttpClient.builder();
        S3Client s3 = S3Client.builder().httpClientBuilder(http).build();
        DynamoDbClient dynamo = DynamoDbClient.builder().httpClientBuilder(http).build();
        SqsClient sqs = SqsClient.builder().httpClientBuilder(http).build();
        return service(config,
                new S3RawArchive(s3, config.archiveBucket()),
                new DynamoDbDedupStore(dynamo, config.dedupTable(), config.dedupLease(), config.dedupTtl()),
                new SqsEventQueue(sqs, config.queueUrl(), JSON));
    }
}
