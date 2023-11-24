package uk.gov.justice.digital.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;
import uk.gov.justice.digital.clients.dynamo.DefaultDynamoDbClientBuilderBuilder;
import uk.gov.justice.digital.clients.dynamo.DynamoDbClientBuilder;
import uk.gov.justice.digital.clients.stepfunctions.DefaultStepFunctionsClientBuilder;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClientBuilder;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

public class StepFunctionDMSNotificationLambda implements RequestHandler<Map<String, Object>, Void> {

    private final DynamoDbClientBuilder dynamoDbClient;
    private final StepFunctionsClientBuilder stepFunctionsClientBuilder;
    private final Clock clock;

    public StepFunctionDMSNotificationLambda() {
        this.dynamoDbClient = new DefaultDynamoDbClientBuilderBuilder();
        this.stepFunctionsClientBuilder = new DefaultStepFunctionsClientBuilder();
        this.clock = Clock.systemUTC();
    }

    public StepFunctionDMSNotificationLambda(
            DynamoDbClientBuilder dynamoDbClientBuilder,
            StepFunctionsClientBuilder stepFunctionsClientBuilder,
            Clock clock) {
        this.dynamoDbClient = dynamoDbClientBuilder;
        this.stepFunctionsClientBuilder = stepFunctionsClientBuilder;
        this.clock = clock;
    }

    final static String DEFAULT_REGION = "eu-west-2";
    // Optional aws region. Defaults to eu-west-2
    final static String AWS_REGION_KEY = "awsRegion";
    final static String DYNAMO_DB_TABLE = "dpr-step-functions-states";
    // Optional task token. Present for a DMS start action and will be saved in DynamoDB.
    // When absent, means the Lambda should stop the step-function using the saved token in Dynamo.
    final static String TASK_TOKEN_KEY = "token";
    final static String REPLICATION_TASK_ARN_KEY = "replicationTaskArn";
    final static String CREATED_AT_KEY = "createdAt";
    final static String CLOUDWATCH_EVENT_RESOURCES_KEY = "resources";

    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {

        LambdaLogger logger = context.getLogger();
        final String region = getOptionalString(event, AWS_REGION_KEY).orElse(DEFAULT_REGION);
        final String inputToken = getOptionalString(event, TASK_TOKEN_KEY).orElse("");
        final String taskArn = getOptionalString(event, REPLICATION_TASK_ARN_KEY)
                .orElseGet(() -> getCloudWatchEventTaskArn(event));

        AmazonDynamoDB dynamoDb = dynamoDbClient.buildClient(region);

        if (inputToken.isEmpty()) {
            processStopEvent(logger, region, taskArn, dynamoDb);
        } else {
            logger.log(String.format("Saving token %s to Dynamo table", inputToken));
            registerTaskToken(inputToken, taskArn, dynamoDb);
        }

        logger.log("Done");

        return null;
    }

    private void processStopEvent(LambdaLogger logger, String region, String taskArn, AmazonDynamoDB dynamoDb) {
        logger.log("Getting token from Dynamo table");
        Map<String, AttributeValue> itemKey = Map.of(REPLICATION_TASK_ARN_KEY, new AttributeValue(taskArn));

        Optional<String> retrievedToken = retrieveToken(dynamoDb, itemKey);

        logger.log(String.format("Notifying step functions of success using token %s", retrievedToken));
        retrievedToken.ifPresent(token -> notifyStepFunction(region, token));

        logger.log("Deleting retrieved token from Dynamo table");
        deleteToken(dynamoDb, itemKey);
    }

    private static void deleteToken(AmazonDynamoDB dynamoDb, Map<String, AttributeValue> itemKey) {
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest(DYNAMO_DB_TABLE, itemKey);
        dynamoDb.deleteItem(deleteItemRequest);
    }

    private void notifyStepFunction(String region, String retrievedToken) {
        final AWSStepFunctions stepFunctions = stepFunctionsClientBuilder.buildClient(region);
        SendTaskSuccessRequest taskSuccessRequest = new SendTaskSuccessRequest()
                .withTaskToken(retrievedToken)
                .withOutput("{}");
        stepFunctions.sendTaskSuccess(taskSuccessRequest);
    }

    private static Optional<String> retrieveToken(AmazonDynamoDB dynamoDb, Map<String, AttributeValue> itemKey) {
        GetItemRequest getTokenRequest = new GetItemRequest(DYNAMO_DB_TABLE, itemKey);
        GetItemResult getTokenResult = dynamoDb.getItem(getTokenRequest);

        return Optional.ofNullable(getTokenResult.getItem().get(TASK_TOKEN_KEY)).map(AttributeValue::getS);
    }

    private void registerTaskToken(String inputToken, String taskArn, AmazonDynamoDB dynamoDb) {
        String createdAt = LocalDateTime.now(clock).format(DateTimeFormatter.ISO_DATE_TIME);
        Map<String, AttributeValue> item = Map
                .of(
                        REPLICATION_TASK_ARN_KEY, new AttributeValue(taskArn),
                        TASK_TOKEN_KEY, new AttributeValue(inputToken),
                        CREATED_AT_KEY, new AttributeValue(createdAt)
                );
        PutItemRequest putTokenRequest = new PutItemRequest(DYNAMO_DB_TABLE, item);
        dynamoDb.putItem(putTokenRequest);
    }

    private static Optional<String> getOptionalString(Map<String, Object> event, String key) {
        return Optional.ofNullable(event.get(key)).map(obj -> (String) obj);
    }

    private String getCloudWatchEventTaskArn(Map<String, Object> event) {
        ArrayList<String> resources = (ArrayList<String>) event.get(CLOUDWATCH_EVENT_RESOURCES_KEY);
        return resources.get(0);
    }

}
