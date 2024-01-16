package uk.gov.justice.digital.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import uk.gov.justice.digital.clients.glue.DefaultGlueProvider;
import uk.gov.justice.digital.clients.glue.GlueClient;
import uk.gov.justice.digital.clients.stepfunctions.DefaultStepFunctionsProvider;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.services.GlueService;

import java.util.Map;

import static uk.gov.justice.digital.common.Utils.TASK_TOKEN_KEY;
import static uk.gov.justice.digital.common.Utils.getOrThrow;

/**
 * This Lambda stops a running glue job and notifies the step function whether it succeeded or failed
 * using the following JSON event structure
 * <pre>
 * {
 *    "glueJob": "some-source-bucket",
 *    "token": "step-functions-task-token"
 * }
 * </pre>
 */
public class StopGlueJobLambda implements RequestHandler<Map<String, Object>, Void> {

    public final static String GLUE_JOB_NAME = "glueJobName";

    private final GlueService service;

    @SuppressWarnings("unused")
    public StopGlueJobLambda() {
        GlueClient glueClient = new GlueClient(new DefaultGlueProvider());
        StepFunctionsClient stepFunctionsClient = new StepFunctionsClient(new DefaultStepFunctionsProvider());
        this.service = new GlueService(glueClient, stepFunctionsClient);
    }

    public StopGlueJobLambda(GlueClient glueClient, StepFunctionsClient stepFunctionsClient) {
        this.service = new GlueService(glueClient, stepFunctionsClient);
    }

    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {
        LambdaLogger logger = context.getLogger();
        final String glueJobName = getOrThrow(event, GLUE_JOB_NAME, String.class);
        final String inputToken = getOrThrow(event, TASK_TOKEN_KEY, String.class);

        service.stopJob(logger, glueJobName, inputToken);

        return null;
    }

}
