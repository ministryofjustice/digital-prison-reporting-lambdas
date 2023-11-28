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
import java.util.Map;

import static uk.gov.justice.digital.common.Utils.*;

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
 *     "replicationTaskArn": "DMS replication task ARN"
 *  }
 * </pre>
 * <p>When received, the Lambda saves the token to dynamoDB using the replicationTaskArn value as key.
 * <li>ProcessDMSStoppage mode:
 * <p>This mode is invoked by the resulting cloudwatch event after the DMS load completes. The sample request is:
 *
 * <pre>
 *  {
 *     "resources": [ "DMS replication task ARN" ]
 *  }
 * </pre>
 * <p>
 * This causes the Lambda to retrieve the taskToken from dynamoDB,
 * send a success notification to AWS the step function that the DMS load has completed,
 * and finally deletes the taskToken from dynamoDB.
 * </ul>
 */
public class StepFunctionDMSNotificationLambda implements RequestHandler<Map<String, Object>, Void> {

    final static String DYNAMO_DB_TABLE = "dpr-step-function-tokens";

    public final static String CLOUDWATCH_EVENT_RESOURCES_KEY = "resources";

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
        // When absent, means the Lambda should stop the step-function using the saved token in Dynamo.
        final String inputToken = getOptionalString(event, TASK_TOKEN_KEY).orElse("");
        final String taskArn = getOptionalString(event, REPLICATION_TASK_ARN_KEY)
                .orElseGet(() -> getCloudWatchEventTaskArn(event));

        if (inputToken.isEmpty()) {
            service.processStopEvent(logger, DYNAMO_DB_TABLE, REPLICATION_TASK_ARN_KEY, taskArn);
        } else {
            logger.log(String.format("Saving token %s to Dynamo table", inputToken), LogLevel.INFO);
            service.registerTaskToken(inputToken, taskArn, DYNAMO_DB_TABLE);
        }

        logger.log("Done", LogLevel.INFO);

        return null;
    }

    @SuppressWarnings("unchecked")
    private String getCloudWatchEventTaskArn(Map<String, Object> event) {
        ArrayList<String> resources = (ArrayList<String>) event.get(CLOUDWATCH_EVENT_RESOURCES_KEY);
        return resources.get(0);
    }

}
