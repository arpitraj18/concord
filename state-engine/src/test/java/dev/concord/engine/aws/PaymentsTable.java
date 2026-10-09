package dev.concord.engine.aws;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

/** Same schema as infra/terraform/payments.tf. */
public final class PaymentsTable {

    private PaymentsTable() {
    }

    public static void create(DynamoDbClient dynamo, String table) {
        dynamo.createTable(b -> b
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .keySchema(key(DynamoDbPaymentStore.PK, KeyType.HASH), key(DynamoDbPaymentStore.SK, KeyType.RANGE))
                .attributeDefinitions(attr(DynamoDbPaymentStore.PK), attr(DynamoDbPaymentStore.SK),
                        attr(DynamoDbPaymentStore.GSI1_PK), attr(DynamoDbPaymentStore.GSI1_SK),
                        attr(DynamoDbPaymentStore.GSI2_PK), attr(DynamoDbPaymentStore.GSI2_SK))
                .globalSecondaryIndexes(
                        index(DynamoDbPaymentStore.GSI1, DynamoDbPaymentStore.GSI1_PK, DynamoDbPaymentStore.GSI1_SK),
                        index(DynamoDbPaymentStore.GSI2, DynamoDbPaymentStore.GSI2_PK, DynamoDbPaymentStore.GSI2_SK))
                .streamSpecification(b2 -> b2.streamEnabled(true).streamViewType(StreamViewType.NEW_IMAGE)));
    }

    private static GlobalSecondaryIndex index(String name, String pk, String sk) {
        return GlobalSecondaryIndex.builder().indexName(name)
                .keySchema(key(pk, KeyType.HASH), key(sk, KeyType.RANGE))
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build();
    }

    private static KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }

    private static AttributeDefinition attr(String name) {
        return AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.S).build();
    }
}
