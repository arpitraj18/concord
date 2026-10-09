package dev.concord.delivery;

import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.store.OutboxEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

/**
 * Change data capture: reads the payments table's stream and hands every new OUTBOX item to a
 * consumer (the delivery worker). The stream keeps changes to one partition key in order, and an
 * OUTBOX item is only ever written in the same transaction as the state change it describes, so this
 * is the reliable half of the transactional outbox.
 *
 * <p>Positions are kept in memory: after a restart the reader starts again from the oldest record
 * still in the stream (24 h), so some entries are re-delivered. That is safe because delivery is
 * at-least-once by contract and the merchant dedupes. In AWS, a Lambda event source mapping can do
 * the same with managed checkpoints.
 */
public final class DynamoDbStreamsOutboxReader implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbStreamsOutboxReader.class);

    private final DynamoDbStreamsClient streams;
    private final String streamArn;
    private final Consumer<OutboxEntry> consumer;
    private final Map<String, String> iterators = new HashMap<>();
    private volatile boolean running = true;

    public DynamoDbStreamsOutboxReader(DynamoDbStreamsClient streams, String streamArn, Consumer<OutboxEntry> consumer) {
        this.streams = streams;
        this.streamArn = streamArn;
        this.consumer = consumer;
    }

    /** One pass over every shard. Returns the number of outbox entries handed on. */
    public int pollOnce() {
        int handed = 0;
        List<Shard> shards = streams.describeStream(b -> b.streamArn(streamArn)).streamDescription().shards();
        for (Shard shard : shards) {
            String iterator = iterators.computeIfAbsent(shard.shardId(), id -> streams.getShardIterator(b -> b
                    .streamArn(streamArn).shardId(id).shardIteratorType(ShardIteratorType.TRIM_HORIZON)).shardIterator());
            if (iterator == null) {
                continue;
            }
            GetRecordsResponse response = streams.getRecords(b -> b.shardIterator(iterator).limit(1000));
            for (Record r : response.records()) {
                Map<String, AttributeValue> image = r.dynamodb().newImage();
                if (image != null && !image.isEmpty() && DynamoDbPaymentStore.isOutboxItem(image)) {
                    consumer.accept(DynamoDbPaymentStore.toOutbox(image));
                    handed++;
                }
            }
            iterators.put(shard.shardId(), response.nextShardIterator());
        }
        return handed;
    }

    @Override
    public void run() {
        while (running) {
            try {
                if (pollOnce() == 0) {
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.error("stream poll failed; retrying", e);
            }
        }
    }

    public void stop() {
        running = false;
    }
}
