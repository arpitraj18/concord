package dev.concord.ingest.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.IngestionService;
import dev.concord.ingest.Wiring;
import dev.concord.ingest.archive.InMemoryRawArchive;
import dev.concord.ingest.aws.DynamoDbDedupStore;
import dev.concord.ingest.aws.DynamoDbLocal;
import dev.concord.ingest.aws.ElasticMq;
import dev.concord.ingest.aws.SqsEventQueue;
import dev.concord.ingest.local.LocalIngestServer;
import dev.concord.ingest.queue.IngestedEvent;
import dev.concord.ingest.verify.Secrets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Same hostile traffic, but dedup runs on DynamoDB (Local) conditional writes and events land on an
 * SQS FIFO queue (ElasticMQ). The queue is then drained like the Phase 2 state engine will drain it.
 */
class AwsAdaptersPipelineTest {

    @Test
    void exactlyOnceThroughDynamoDbAndSqsFifo() throws Exception {
        ObjectMapper json = new ObjectMapper();
        IngestConfig config = new IngestConfig(Map.of(Gateway.A, Secrets.of("whsec_A")), Duration.ofMinutes(5),
                Duration.ofSeconds(30), Duration.ofDays(7), "unused", "concord-dedup", "unused");

        DynamoDbClient dynamo = DynamoDbLocal.client();
        DynamoDbLocal.createDedupTable(dynamo, "concord-dedup");
        try (ElasticMq sqs = ElasticMq.start()) {
            String queueUrl = sqs.createFifoQueue("concord-events.fifo");
            InMemoryRawArchive archive = new InMemoryRawArchive();
            IngestionService service = Wiring.service(config, archive,
                    new DynamoDbDedupStore(dynamo, "concord-dedup", config.dedupLease(), config.dedupTtl()),
                    new SqsEventQueue(sqs.client(), queueUrl, json));

            try (LocalIngestServer ingest = new LocalIngestServer(0, service, null, Clock.systemUTC());
                 SimulatorHarness sim = new SimulatorHarness("A", "whsec_A")) {
                Map<String, Object> scenario = new HashMap<>(SimulatorToIngestionTest.chaosScenario(
                        "http://127.0.0.1:" + ingest.port() + "/webhooks/a"));
                scenario.put("generate", Map.of("count", 300, "spreadMs", 20000, "stepGapMs", 3000));
                String runId = sim.startRun(scenario);
                sim.awaitCompleted(runId, Duration.ofSeconds(60));
                JsonNode plan = sim.get("/sim/runs/" + runId + "/plan");

                List<IngestedEvent> drained = drain(sqs, queueUrl, json);
                Set<String> expected = StreamSupport.stream(plan.get("deliveries").spliterator(), false)
                        .filter(d -> !d.get("dropped").asBoolean())
                        .map(d -> d.get("eventId").asText())
                        .collect(Collectors.toSet());

                assertThat(drained).extracting(IngestedEvent::dedupKey).doesNotHaveDuplicates();
                assertThat(drained.stream().map(IngestedEvent::gatewayEventId).collect(Collectors.toSet()))
                        .isEqualTo(expected);
                assertThat(archive.size()).isGreaterThan(drained.size());
            }
        } finally {
            dynamo.close();
        }
    }

    private static List<IngestedEvent> drain(ElasticMq sqs, String queueUrl, ObjectMapper json) throws Exception {
        List<IngestedEvent> out = new ArrayList<>();
        int emptyPolls = 0;
        while (emptyPolls < 3) {
            List<Message> batch = sqs.client().receiveMessage(b -> b.queueUrl(queueUrl).maxNumberOfMessages(10)
                    .waitTimeSeconds(1)).messages();
            if (batch.isEmpty()) {
                emptyPolls++;
                continue;
            }
            emptyPolls = 0;
            for (Message m : batch) {
                out.add(json.readValue(m.body(), IngestedEvent.class));
                sqs.client().deleteMessage(b -> b.queueUrl(queueUrl).receiptHandle(m.receiptHandle()));
            }
        }
        return out;
    }
}
