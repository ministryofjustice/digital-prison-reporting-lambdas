package uk.gov.justice.digital.services;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.dynamo.DynamoDbClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.common.TaskDetail;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class StepFunctionDMSNotificationService {

    private final DynamoDbClient dynamoDbClient;
    private final StepFunctionsClient stepFunctionsClient;
    private final Clock clock;

    public final static String DMS_TASK_STOPPAGE_ERROR_EVENT_ID = "DMS-EVENT-0078";
    public final static String DMS_TASK_SUCCESS_EVENT_ID = "DMS-EVENT-0079";

    public StepFunctionDMSNotificationService(
            DynamoDbClient dynamoDbClient,
            StepFunctionsClient stepFunctionsClient,
            Clock clock
    ) {
        this.dynamoDbClient = dynamoDbClient;
        this.stepFunctionsClient = stepFunctionsClient;
        this.clock = clock;
    }

    public void processFailureEvent(LambdaLogger logger, String dynamoTable, String taskKey, String taskArn, String failureMessage) {
        processEvent(logger, dynamoTable, taskKey, taskArn, taskDetail -> {
            String errorMessage = String.format("Failing function due to failure [%s] for DMS task [%s]", failureMessage, taskArn);
            logger.log(errorMessage, LogLevel.ERROR);
            processFailureEvent(logger, taskDetail, errorMessage);
        });
    }

    public void processStopEvent(LambdaLogger logger, String dynamoTable, String taskKey, String taskArn, String eventId) {
        processEvent(logger, dynamoTable, taskKey, taskArn, taskDetail -> {
            if (eventId.equalsIgnoreCase(DMS_TASK_STOPPAGE_ERROR_EVENT_ID)) {
                if (taskDetail.ignoreFailure()) {
                    logger.log(String.format("Ignoring failure [%s] for DMS task [%s]", DMS_TASK_STOPPAGE_ERROR_EVENT_ID, taskArn), LogLevel.INFO);
                    processSuccessfulStopEvent(logger, taskDetail);
                } else {
                    String errorMessage = String.format("Failing function due to stoppage error [%s] for DMS task [%s]", DMS_TASK_STOPPAGE_ERROR_EVENT_ID, taskArn);
                    logger.log(errorMessage, LogLevel.ERROR);
                    processFailureEvent(logger, taskDetail, errorMessage);
                }
            } else {
                processSuccessfulStopEvent(logger, taskDetail);
            }
        });
    }

    public void registerTaskDetails(LambdaLogger logger, String inputToken, String taskArn, boolean ignoreTaskFailure, String table, Long tokenExpiryDays) {
        logger.log(String.format("Saving token %s to Dynamo table", inputToken), LogLevel.INFO);
        LocalDateTime now = LocalDateTime.now(clock);
        String createdAt = now.format(DateTimeFormatter.ISO_DATE_TIME);
        long expireAt = now.plusDays(tokenExpiryDays).toEpochSecond(ZoneOffset.UTC);

        dynamoDbClient.saveTaskDetails(table, taskArn, inputToken, ignoreTaskFailure, expireAt, createdAt);
    }

    private void processEvent(LambdaLogger logger, String dynamoTable, String taskKey, String taskArn, Consumer<TaskDetail> notifyStepFunction) {
        Map<String, AttributeValue> itemKey = Map.of(taskKey, new AttributeValue(taskArn));
        logger.log("Getting token from Dynamo table", LogLevel.INFO);
        Optional<TaskDetail> optionalTaskDetail = dynamoDbClient.retrieveTaskDetail(dynamoTable, itemKey);

        optionalTaskDetail.ifPresentOrElse(
                notifyStepFunction,
                () -> {
                    throw new RuntimeException("No Task details found in Dynamo table for " + taskArn);
                }
        );

        logger.log("Deleting retrieved token from Dynamo table", LogLevel.INFO);
        dynamoDbClient.deleteToken(dynamoTable, itemKey);
    }

    private void processSuccessfulStopEvent(LambdaLogger logger, TaskDetail taskDetail) {
        String taskToken = taskDetail.getToken();
        logger.log(String.format("Notifying step functions of success using token [%s]", taskToken), LogLevel.INFO);
        stepFunctionsClient.notifyStepFunctionSuccess(taskToken);
    }

    private void processFailureEvent(LambdaLogger logger, TaskDetail taskDetail, String error) {
        String taskToken = taskDetail.getToken();
        logger.log(String.format("Notifying step functions of failure using token [%s]", taskToken), LogLevel.INFO);
        stepFunctionsClient.notifyStepFunctionFailure(taskToken, error);
    }
}
