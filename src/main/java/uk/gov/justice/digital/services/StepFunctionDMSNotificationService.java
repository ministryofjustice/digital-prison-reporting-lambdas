package uk.gov.justice.digital.services;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.dynamo.DynamoDbClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

public class StepFunctionDMSNotificationService {

    private final DynamoDbClient dynamoDbClient;
    private final StepFunctionsClient stepFunctionsClient;
    private final Clock clock;

    public StepFunctionDMSNotificationService(
            DynamoDbClient dynamoDbClient,
            StepFunctionsClient stepFunctionsClient,
            Clock clock
    ) {
        this.dynamoDbClient = dynamoDbClient;
        this.stepFunctionsClient = stepFunctionsClient;
        this.clock = clock;
    }

    public void processStopEvent(LambdaLogger logger, String dynamoTable, String taskKey, String taskArn) {
        Map<String, AttributeValue> itemKey = Map.of(taskKey, new AttributeValue(taskArn));
        logger.log("Getting token from Dynamo table", LogLevel.INFO);
        Optional<String> retrievedToken = dynamoDbClient.retrieveToken(dynamoTable, itemKey);

        retrievedToken.ifPresent(token -> {
                    logger.log(String.format("Notifying step functions of success using token %s", token), LogLevel.INFO);
                    stepFunctionsClient.notifyStepFunctionSuccess(token);
                }
        );

        logger.log("Deleting retrieved token from Dynamo table", LogLevel.INFO);
        dynamoDbClient.deleteToken(dynamoTable, itemKey);
    }

    public void registerTaskToken(String inputToken, String taskArn, String table, Long tokenExpiryDays) {
        LocalDateTime now = LocalDateTime.now(clock);
        String createdAt = now.format(DateTimeFormatter.ISO_DATE_TIME);
        long expireAt = now.plusDays(tokenExpiryDays).toEpochSecond(ZoneOffset.UTC);

        dynamoDbClient.saveToken(table, taskArn, inputToken, expireAt, createdAt);
    }
}
