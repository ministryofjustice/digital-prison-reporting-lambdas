package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.glue.GlueClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;

public class GlueService {

    private final GlueClient glueClient;
    private final StepFunctionsClient stepFunctionsClient;

    public GlueService(
            GlueClient glueClient,
            StepFunctionsClient stepFunctionsClient
    ) {
        this.glueClient = glueClient;
        this.stepFunctionsClient = stepFunctionsClient;
    }

    public void stopJob(LambdaLogger logger, String jobName, String token) {
        try {
            logger.log(String.format("Stopping Glue job %s", jobName), LogLevel.INFO);
            glueClient.stopJob(jobName);
            logger.log(String.format("Stopped Glue job %s", jobName), LogLevel.INFO);
            logger.log(String.format("Notifying step functions of success using token %s", token), LogLevel.INFO);
            stepFunctionsClient.notifyStepFunctionSuccess(token);
        } catch (Exception ex) {
            logger.log(String.format("Failed to stop glue job %s", ex.getMessage()), LogLevel.ERROR);
            logger.log(String.format("Notifying step functions of failure using token %s", token), LogLevel.INFO);
            stepFunctionsClient.notifyStepFunctionFailure(token, ex.getMessage());
            System.exit(1);
        }
    }
}
