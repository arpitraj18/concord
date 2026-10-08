package dev.concord.ingest.aws;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** DynamoDB Local running inside the test JVM: the real DynamoDB API and condition evaluation, no Docker. */
public final class DynamoDbLocal {

    private DynamoDbLocal() {
    }

    public static DynamoDbClient client() {
        return DynamoDBEmbedded.create().dynamoDbClient();
    }

    /** Same key schema as infra/terraform/dynamodb.tf. */
    public static void createDedupTable(DynamoDbClient dynamo, String table) {
        dynamo.createTable(b -> b
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .keySchema(KeySchemaElement.builder().attributeName(DynamoDbDedupStore.KEY).keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(DynamoDbDedupStore.KEY).attributeType(ScalarAttributeType.S).build()));
    }
}
