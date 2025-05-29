package uk.gov.justice.digital.clients.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import uk.gov.justice.digital.common.TaskDetail;

import java.util.Map;
import java.util.Optional;

import static uk.gov.justice.digital.common.Utils.REPLICATION_TASK_ARN_KEY;
import static uk.gov.justice.digital.common.Utils.TASK_TOKEN_KEY;
import static uk.gov.justice.digital.common.Utils.IGNORE_DMS_TASK_FAILURE_KEY;

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

    public Optional<TaskDetail> retrieveTaskDetail(String table, Map<String, AttributeValue> itemKey) {
        GetItemRequest getTokenRequest = new GetItemRequest(table, itemKey);
        GetItemResult getTokenResult = dynamoDbClient.getItem(getTokenRequest);

        Map<String, AttributeValue> item = getTokenResult.getItem();
        Optional<String> optionalTaskKey = Optional.ofNullable(item.get(TASK_TOKEN_KEY)).map(AttributeValue::getS);
        boolean ignoreTaskFailure = item.getOrDefault(IGNORE_DMS_TASK_FAILURE_KEY, new AttributeValue().withBOOL(false)).getBOOL();

        return optionalTaskKey.map(taskKey -> new TaskDetail(taskKey, ignoreTaskFailure));
    }

    public void saveTaskDetails(String table, String taskArn, String inputToken, boolean ignoreDmsTaskFailure, long expireAt, String createdAt) {
        AttributeValue expiryAttribute = new AttributeValue().withN(String.valueOf(expireAt));
        AttributeValue ignoreDmsTaskFailureAttribute = new AttributeValue().withBOOL(ignoreDmsTaskFailure);
        Map<String, AttributeValue> item = Map
                .of(
                        REPLICATION_TASK_ARN_KEY, new AttributeValue(taskArn),
                        IGNORE_DMS_TASK_FAILURE_KEY, ignoreDmsTaskFailureAttribute,
                        TASK_TOKEN_KEY, new AttributeValue(inputToken),
                        CREATED_AT_KEY, new AttributeValue(createdAt),
                        EXPIRE_AT_KEY, expiryAttribute
                );
        PutItemRequest putTokenRequest = new PutItemRequest(table, item);
        dynamoDbClient.putItem(putTokenRequest);
    }
}
