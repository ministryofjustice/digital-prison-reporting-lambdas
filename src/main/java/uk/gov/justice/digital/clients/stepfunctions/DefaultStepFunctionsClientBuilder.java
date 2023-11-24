package uk.gov.justice.digital.clients.stepfunctions;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;

public class DefaultStepFunctionsClientBuilder implements StepFunctionsClientBuilder {

    public DefaultStepFunctionsClientBuilder() {}

    @Override
    public AWSStepFunctions buildClient(String region) {
        return AWSStepFunctionsClientBuilder.standard().withRegion(region).build();
    }
}
