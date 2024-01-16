package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.dynamo.DynamoDbClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.services.test.Fixture.fixedClock;
import static uk.gov.justice.digital.services.test.Fixture.fixedDateTime;

@ExtendWith(MockitoExtension.class)
class StepFunctionDMSNotificationServiceTest {

    private static final String TABLE = "dynamo-table";
    private static final String TOKEN = "token";
    private static final String TASK_ARN = "task-arn";

    @Mock
    DynamoDbClient mockDynamoDbClient;
    @Mock
    StepFunctionsClient mockStepFunctionsClient;
    @Mock
    LambdaLogger mockLambdaLogger;

    private StepFunctionDMSNotificationService undertest;

    @BeforeEach
    public void setup() {
        undertest = new StepFunctionDMSNotificationService(mockDynamoDbClient, mockStepFunctionsClient, fixedClock);
    }

    @Test
    public void processStopEventShouldNotifyStepFunctionsAndDeleteToken() {
        doNothing().when(mockLambdaLogger).log(anyString(), any());
        when(mockDynamoDbClient.retrieveToken(eq(TABLE), any())).thenReturn(Optional.of(TOKEN));

        undertest.processStopEvent(mockLambdaLogger, TABLE, "task-key", TASK_ARN);

        verify(mockStepFunctionsClient, times(1)).notifyStepFunctionSuccess(eq(TOKEN));
        verify(mockDynamoDbClient, times(1)).deleteToken(eq(TABLE), any());
    }

    @Test
    public void processStopEventShouldNotNotifyStepFunctionsWhenTokenIsNotPresent() {
        doNothing().when(mockLambdaLogger).log(anyString(), any());
        when(mockDynamoDbClient.retrieveToken(eq(TABLE), any())).thenReturn(Optional.empty());

        undertest.processStopEvent(mockLambdaLogger, TABLE, "task-key", TASK_ARN);

        verify(mockDynamoDbClient, times(1)).deleteToken(eq(TABLE), any());
        verifyNoInteractions(mockStepFunctionsClient);
    }

    @Test
    public void registerTaskTokenShouldSaveToken() {

        undertest.registerTaskToken(TOKEN, TASK_ARN, TABLE);

        verify(mockDynamoDbClient, times(1))
                .saveToken(
                        eq(TABLE),
                        eq(TASK_ARN),
                        eq(TOKEN),
                        eq(fixedDateTime.plusDays(1).toEpochSecond(ZoneOffset.UTC)),
                        eq(fixedDateTime.format(DateTimeFormatter.ISO_DATE_TIME))
                );
    }

}