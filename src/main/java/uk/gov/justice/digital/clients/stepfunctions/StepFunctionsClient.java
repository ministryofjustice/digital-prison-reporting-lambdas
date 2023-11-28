package uk.gov.justice.digital.clients.stepfunctions;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;

public class StepFunctionsClient {

    private final AWSStepFunctions stepFunctions;

    public StepFunctionsClient(StepFunctionsProvider stepFunctionsProvider) {
        this.stepFunctions = stepFunctionsProvider.buildClient();
    }

    public void notifyStepFunction(String retrievedToken) {
        SendTaskSuccessRequest taskSuccessRequest = new SendTaskSuccessRequest()
                .withTaskToken(retrievedToken)
                .withOutput("{}");
        stepFunctions.sendTaskSuccess(taskSuccessRequest);
    }
}
