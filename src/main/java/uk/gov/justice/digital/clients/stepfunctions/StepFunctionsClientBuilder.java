package uk.gov.justice.digital.clients.stepfunctions;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;

public interface StepFunctionsClientBuilder {
    AWSStepFunctions buildClient(String region);
}
