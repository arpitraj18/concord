package dev.concord.ingest.aws;

import dev.concord.ingest.dedup.ClaimOutcome;
import dev.concord.ingest.dedup.DedupStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;

/**
 * Dedup table: one item per event, {@code dedup_key} is the partition key, {@code expires_at} is the TTL
 * attribute.
 *
 * <p>The whole claim decision is <b>one conditional PutItem</b>, so two deliveries of the same event
 * racing on two Lambda instances cannot both win. The condition allows the write when the key is new,
 * when the old item has expired (TTL deletion is lazy, so expired items can still be present), or when
 * the old item is an abandoned claim older than the lease.
 */
public final class DynamoDbDedupStore implements DedupStore {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbDedupStore.class);

    static final String KEY = "dedup_key";
    static final String STATUS = "status";
    static final String TOKEN = "claim_token";
    static final String CLAIMED_AT = "claimed_at";
    static final String ENQUEUED_AT = "enqueued_at";
    static final String EXPIRES_AT = "expires_at";
    static final String CLAIMED = "CLAIMED";
    static final String ENQUEUED = "ENQUEUED";

    private static final String CLAIM_CONDITION = "attribute_not_exists(" + KEY + ")"
            + " OR " + EXPIRES_AT + " < :nowSec"
            + " OR (#st = :claimed AND " + CLAIMED_AT + " < :staleBefore)";

    private final DynamoDbClient dynamo;
    private final String table;
    private final Duration lease;
    private final Duration ttl;

    public DynamoDbDedupStore(DynamoDbClient dynamo, String table, Duration lease, Duration ttl) {
        this.dynamo = dynamo;
        this.table = table;
        this.lease = lease;
        this.ttl = ttl;
    }

    @Override
    public ClaimOutcome claim(String dedupKey, String token, Instant now) {
        long nowSec = now.getEpochSecond();
        try {
            PutItemResponse response = dynamo.putItem(b -> b
                    .tableName(table)
                    .item(Map.of(
                            KEY, s(dedupKey),
                            STATUS, s(CLAIMED),
                            TOKEN, s(token),
                            CLAIMED_AT, n(now.toEpochMilli()),
                            EXPIRES_AT, n(now.plus(ttl).getEpochSecond())))
                    .conditionExpression(CLAIM_CONDITION)
                    .expressionAttributeNames(Map.of("#st", STATUS))
                    .expressionAttributeValues(Map.of(
                            ":nowSec", n(nowSec),
                            ":claimed", s(CLAIMED),
                            ":staleBefore", n(now.minus(lease).toEpochMilli())))
                    .returnValues(ReturnValue.ALL_OLD)
                    .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD));
            Map<String, AttributeValue> old = response.attributes();
            boolean replacedLiveClaim = old != null && !old.isEmpty()
                    && Long.parseLong(old.get(EXPIRES_AT).n()) >= nowSec;
            return replacedLiveClaim ? ClaimOutcome.TAKEN_OVER : ClaimOutcome.CLAIMED;
        } catch (ConditionalCheckFailedException e) {
            Map<String, AttributeValue> existing = e.hasItem() ? e.item() : read(dedupKey);
            String status = existing == null || existing.get(STATUS) == null ? null : existing.get(STATUS).s();
            return ENQUEUED.equals(status) ? ClaimOutcome.DUPLICATE : ClaimOutcome.IN_FLIGHT;
        }
    }

    private Map<String, AttributeValue> read(String dedupKey) {
        return dynamo.getItem(b -> b.tableName(table).key(Map.of(KEY, s(dedupKey))).consistentRead(true)).item();
    }

    @Override
    public void markEnqueued(String dedupKey, String token, Instant now) {
        try {
            dynamo.updateItem(b -> b
                    .tableName(table)
                    .key(Map.of(KEY, s(dedupKey)))
                    .updateExpression("SET #st = :enqueued, " + ENQUEUED_AT + " = :now")
                    .conditionExpression(TOKEN + " = :token")
                    .expressionAttributeNames(Map.of("#st", STATUS))
                    .expressionAttributeValues(Map.of(
                            ":enqueued", s(ENQUEUED),
                            ":now", n(now.toEpochMilli()),
                            ":token", s(token))));
        } catch (ConditionalCheckFailedException e) {
            log.warn("lost claim on {} before marking it enqueued", dedupKey);
        }
    }

    @Override
    public void release(String dedupKey, String token) {
        try {
            dynamo.deleteItem(b -> b
                    .tableName(table)
                    .key(Map.of(KEY, s(dedupKey)))
                    .conditionExpression(TOKEN + " = :token AND #st = :claimed")
                    .expressionAttributeNames(Map.of("#st", STATUS))
                    .expressionAttributeValues(Map.of(":token", s(token), ":claimed", s(CLAIMED))));
        } catch (ConditionalCheckFailedException e) {
            log.debug("claim on {} already gone or taken over", dedupKey);
        }
    }

    private static AttributeValue s(String v) {
        return AttributeValue.fromS(v);
    }

    private static AttributeValue n(long v) {
        return AttributeValue.fromN(Long.toString(v));
    }
}
