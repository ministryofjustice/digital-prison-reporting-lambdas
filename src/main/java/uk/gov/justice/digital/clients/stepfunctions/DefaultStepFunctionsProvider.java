package uk.gov.justice.digital.clients.stepfunctions;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;

import static uk.gov.justice.digital.common.Utils.DEFAULT_DPR_REGION;

public class DefaultStepFunctionsProvider implements StepFunctionsProvider {
    @Override
    public AWSStepFunctions buildClient() {
        return AWSStepFunctionsClientBuilder.standard().withRegion(DEFAULT_DPR_REGION).build();
    }
}
