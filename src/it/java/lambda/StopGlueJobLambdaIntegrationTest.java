package lambda;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchStopJobRunRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.glue.GlueClient;
import uk.gov.justice.digital.clients.glue.GlueProvider;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsProvider;
import uk.gov.justice.digital.lambda.StopGlueJobLambda;

import java.util.HashMap;
import java.util.Map;

import static lambda.test.Fixture.TEST_GLUE_JOB;
import static lambda.test.Fixture.TEST_TOKEN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.Utils.TASK_TOKEN_KEY;
import static uk.gov.justice.digital.lambda.StopGlueJobLambda.GLUE_JOB_NAME;

@ExtendWith(MockitoExtension.class)
public class StopGlueJobLambdaIntegrationTest {

    @Mock
    private Context contextMock;
    @Mock
    private LambdaLogger mockLogger;
    @Mock
    private GlueProvider glueProvider;
    @Mock
    private StepFunctionsProvider stepFunctionProvider;
    @Mock
    private AWSGlue mockAwsGlue;
    @Mock
    private AWSStepFunctions mockStepFunctions;

    @Captor
    ArgumentCaptor<BatchStopJobRunRequest> batchStopJobRequestCaptor;
    @Captor
    ArgumentCaptor<SendTaskSuccessRequest> sendStepFunctionsSuccessRequestCaptor;

    private StopGlueJobLambda underTest;

    @BeforeEach
    public void setup() {
        reset(contextMock, mockLogger, mockAwsGlue, mockStepFunctions);

        when(contextMock.getLogger()).thenReturn(mockLogger);
        doNothing().when(mockLogger).log(anyString(), any());

        when(glueProvider.buildClient()).thenReturn(mockAwsGlue);
        when(stepFunctionProvider.buildClient()).thenReturn(mockStepFunctions);

        underTest = new StopGlueJobLambda(new GlueClient(glueProvider), new StepFunctionsClient(stepFunctionProvider));
    }

    @Test
    public void shouldStopGlueJobAndNotifyStepFunction() {
        Map<String, Object> event = new HashMap<>();
        event.put(GLUE_JOB_NAME, TEST_GLUE_JOB);
        event.put(TASK_TOKEN_KEY, TEST_TOKEN);

        underTest.handleRequest(event, contextMock);

        verify(mockAwsGlue, times(1)).batchStopJobRun(batchStopJobRequestCaptor.capture());
        verify(mockStepFunctions, times(1))
                .sendTaskSuccess(sendStepFunctionsSuccessRequestCaptor.capture());

        assertThat(batchStopJobRequestCaptor.getValue().getJobName(), is(equalTo(TEST_GLUE_JOB)));
        assertThat(sendStepFunctionsSuccessRequestCaptor.getValue().getTaskToken(), is(equalTo(TEST_TOKEN)));
    }

}
