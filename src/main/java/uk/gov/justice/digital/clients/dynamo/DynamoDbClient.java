package uk.gov.justice.digital.clients.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.Map;
import java.util.Optional;

import static uk.gov.justice.digital.common.Utils.REPLICATION_TASK_ARN_KEY;
import static uk.gov.justice.digital.common.Utils.TASK_TOKEN_KEY;

public class DynamoDbClient {

    public final static String CREATED_AT_KEY = "createdAt";
    public final static String EXPIRE_AT_KEY = "expireAt";

    private final AmazonDynamoDB dynamoDbClient;

    public DynamoDbClient(DynamoDbProvider dynamoDbProvider) {
        this.dynamoDbClient = dynamoDbProvider.buildClient();
    }

    public void deleteToken(String table, Map<String, AttributeValue> itemKey) {
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest(table, itemKey);
        dynamoDbClient.deleteItem(deleteItemRequest);
    }

    public Optional<String> retrieveToken(String table, Map<String, AttributeValue> itemKey) {
        GetItemRequest getTokenRequest = new GetItemRequest(table, itemKey);
        GetItemResult getTokenResult = dynamoDbClient.getItem(getTokenRequest);

        return Optional.ofNullable(getTokenResult.getItem().get(TASK_TOKEN_KEY)).map(AttributeValue::getS);
    }

    public void saveToken(String table, String taskArn, String inputToken, long expireAt, String createdAt) {
        AttributeValue expiryAttribute = new AttributeValue().withN(String.valueOf(expireAt));
        Map<String, AttributeValue> item = Map
                .of(
                        REPLICATION_TASK_ARN_KEY, new AttributeValue(taskArn),
                        TASK_TOKEN_KEY, new AttributeValue(inputToken),
                        CREATED_AT_KEY, new AttributeValue(createdAt),
                        EXPIRE_AT_KEY, expiryAttribute
                );
        PutItemRequest putTokenRequest = new PutItemRequest(table, item);
        dynamoDbClient.putItem(putTokenRequest);
    }
}
