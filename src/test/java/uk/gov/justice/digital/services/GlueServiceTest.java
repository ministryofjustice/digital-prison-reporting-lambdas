package uk.gov.justice.digital.services;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.github.stefanbirkner.systemlambda.SystemLambda;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.glue.GlueClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class GlueServiceTest {

    @Mock
    GlueClient mockGlueClient;
    @Mock
    StepFunctionsClient mockStepFunctionsClient;

    @Mock
    LambdaLogger mockLambdaLogger;

    public final static String TEST_TOKEN = "test-token";

    public static final String TEST_JOB_NAME = "test-job-name";

    private GlueService undertest;

    @BeforeEach
    public void setup() {
        undertest = new GlueService(mockGlueClient, mockStepFunctionsClient);
    }

    @Test
    public void shouldCallGlueClientToStopJobAndNotifyStepFunction() {
        assertDoesNotThrow(() -> undertest.stopJob(mockLambdaLogger, TEST_JOB_NAME, TEST_TOKEN));

        verify(mockGlueClient, times(1)).stopJob(TEST_JOB_NAME);
        verify(mockStepFunctionsClient, times(1)).notifyStepFunctionSuccess(TEST_TOKEN);
    }

    @Test
    public void shouldNotifyStepFunctionOfFailureWhenErrorOccursDuringGlueJobTermination() throws Exception {
        String errorMessage = "client exception";
        doThrow(new AmazonClientException(errorMessage)).when(mockGlueClient).stopJob(TEST_JOB_NAME);

        SystemLambda.catchSystemExit(() -> undertest.stopJob(mockLambdaLogger, TEST_JOB_NAME, TEST_TOKEN));

        verify(mockStepFunctionsClient, times(1)).notifyStepFunctionFailure(TEST_TOKEN, errorMessage);
    }

}
