package uk.gov.justice.digital.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.dynamo.DefaultDynamoDbProvider;
import uk.gov.justice.digital.clients.dynamo.DynamoDbClient;
import uk.gov.justice.digital.clients.stepfunctions.DefaultStepFunctionsProvider;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.services.StepFunctionDMSNotificationService;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static uk.gov.justice.digital.common.Utils.TASK_TOKEN_KEY;
import static uk.gov.justice.digital.common.Utils.REPLICATION_TASK_ARN_KEY;
import static uk.gov.justice.digital.common.Utils.IGNORE_DMS_TASK_FAILURE_KEY;
import static uk.gov.justice.digital.common.Utils.TOKEN_EXPIRY_DAYS_KEY;
import static uk.gov.justice.digital.common.Utils.DEFAULT_TOKEN_EXPIRY_DAYS;
import static uk.gov.justice.digital.common.Utils.getOptionalString;
import static uk.gov.justice.digital.common.Utils.getOrThrow;
import static uk.gov.justice.digital.common.Utils.getBoolean;

/**
 * Lambda function to notify AWS step function of DMS load completion.
 * <p>The function runs in two modes (RegisterTaskToken, or ProcessDMSStoppage) depending on the input event received.
 *
 * <ul>
 * <li>RegisterTaskToken mode:
 * <p>This mode is invoked by the AWS step function after the DMS load is started. The event sample issued is:
 *
 * <pre>
 *  {
 *     "token": "some-step-function-task-token",
 *     "ignoreDmsTaskFailure": false,
 *     "replicationTaskArn": "DMS replication task ARN",
 *     "tokenExpiryDays": "5"
 *  }
 * </pre>
 * <p>When received, the Lambda saves the token and the value of ignoreDmsTaskFailure to dynamoDB using the replicationTaskArn value as the key.
 * <li>ProcessDMSStoppage mode:
 * <p>This mode is invoked by the resulting cloudwatch event after the DMS load completes. The sample request is:
 *
 * <pre>
 *  {
 *     "resources": [ "DMS replication task ARN" ],
 *     "detail": {
 *         "eventId": "DMS-EVENT-0079"
 *     }
 *  }
 * </pre>
 * <p>
 * This causes the Lambda to retrieve the taskToken and the value of ignoreDmsTaskFailure from dynamoDB,
 * send a notification to AWS the step function that the DMS load has completed,
 * and finally deletes the taskToken from dynamoDB.
 * If ignoreDmsTaskFailure = true, then a success notification is sent irrespective of the eventId.
 * If ignoreDmsTaskFailure = false and an eventId DMS-EVENT-0078 (DMS task failed) is received, then a failed notification is sent.
 * </ul>
 */
public class StepFunctionDMSNotificationLambda implements RequestHandler<Map<String, Object>, Void> {

    final static String DYNAMO_DB_TABLE = "dpr-step-function-tokens";

    public final static String CLOUDWATCH_EVENT_RESOURCES_KEY = "resources";
    public final static String CLOUDWATCH_EVENT_DETAIL_KEY = "detail";
    public final static String CLOUDWATCH_EVENT_ID_KEY = "eventId";

    private final StepFunctionDMSNotificationService service;

    @SuppressWarnings("unused")
    public StepFunctionDMSNotificationLambda() {
        DynamoDbClient dynamoDbClient = new DynamoDbClient(new DefaultDynamoDbProvider());
        StepFunctionsClient stepFunctionsClient = new StepFunctionsClient(new DefaultStepFunctionsProvider());
        Clock clock = Clock.systemUTC();
        this.service = new StepFunctionDMSNotificationService(dynamoDbClient, stepFunctionsClient, clock);
    }

    public StepFunctionDMSNotificationLambda(
            DynamoDbClient dynamoDbClient,
            StepFunctionsClient stepFunctionsClient,
            Clock clock
    ) {
        this.service = new StepFunctionDMSNotificationService(dynamoDbClient, stepFunctionsClient, clock);
    }

    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {

        LambdaLogger logger = context.getLogger();
        // Optional task token. Present for a DMS start action and will be saved in DynamoDB.
        // When absent, means the Lambda should stop the step-function using the saved token in DynamoDB.
        final String inputToken = getOptionalString(event, TASK_TOKEN_KEY).orElse("");
        final String taskArn = getOptionalString(event, REPLICATION_TASK_ARN_KEY)
                .orElseGet(() -> getCloudWatchEventTaskArn(event));
        final boolean ignoreDmsTaskFailure = getBoolean(event, IGNORE_DMS_TASK_FAILURE_KEY);
        logger.log("Event received: " + event, LogLevel.DEBUG);
        // Optional number of days after which the token should be considered to have expired and will be deleted via TTL.
        // Only used for the start action when saving the token in DynamoDB.
        // When absent, defaults to DEFAULT_TOKEN_EXPIRY_DAYS.
        final Long tokenExpiryDays = getOptionalString(event, TOKEN_EXPIRY_DAYS_KEY)
                .map(Long::parseLong)
                .orElse(DEFAULT_TOKEN_EXPIRY_DAYS);

        if (inputToken.isEmpty()) {
            final String eventId = getStoppageEventId(event);
            service.processStopEvent(logger, DYNAMO_DB_TABLE, REPLICATION_TASK_ARN_KEY, taskArn, eventId);
        } else {
            logger.log(String.format("Saving token %s to Dynamo table", inputToken), LogLevel.INFO);
            service.registerTaskDetails(inputToken, taskArn, ignoreDmsTaskFailure, DYNAMO_DB_TABLE, tokenExpiryDays);
        }

        logger.log("Done", LogLevel.INFO);

        return null;
    }

    @SuppressWarnings("unchecked")
    private String getCloudWatchEventTaskArn(Map<String, Object> event) {
        ArrayList<String> resources = getOrThrow(event, CLOUDWATCH_EVENT_RESOURCES_KEY, ArrayList.class);
        if (resources.isEmpty()) {
            throw new RuntimeException("Could not find DMS task ARN. List of resources is empty");
        } else {
            return resources.get(0);
        }
    }

    @SuppressWarnings("unchecked")
    private String getStoppageEventId(Map<String, Object> event) {
        LinkedHashMap<String, Object> eventDetail = getOrThrow(event, CLOUDWATCH_EVENT_DETAIL_KEY, LinkedHashMap.class);
        return getOrThrow(eventDetail, CLOUDWATCH_EVENT_ID_KEY, String.class);
    }

}
