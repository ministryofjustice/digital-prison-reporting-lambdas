package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.dynamo.DynamoDbClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.common.TaskDetail;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyNoInteractions;
import static uk.gov.justice.digital.services.StepFunctionDMSNotificationService.DMS_TASK_FAILURE_EVENT_ID;
import static uk.gov.justice.digital.services.StepFunctionDMSNotificationService.DMS_TASK_SUCCESS_EVENT_ID;
import static uk.gov.justice.digital.services.test.Fixture.fixedClock;
import static uk.gov.justice.digital.services.test.Fixture.fixedDateTime;

@ExtendWith(MockitoExtension.class)
class StepFunctionDMSNotificationServiceTest {

    private static final String TABLE = "dynamo-table";
    private static final String TOKEN = "token";
    private static final String TASK_ARN = "task-arn";
    private static final Long TOKEN_EXPIRY_DAYS = 4L;

    @Mock
    DynamoDbClient mockDynamoDbClient;
    @Mock
    StepFunctionsClient mockStepFunctionsClient;
    @Mock
    LambdaLogger mockLambdaLogger;

    private StepFunctionDMSNotificationService undertest;

    @BeforeEach
    public void setup() {
        reset(mockDynamoDbClient, mockStepFunctionsClient, mockLambdaLogger);
        undertest = new StepFunctionDMSNotificationService(mockDynamoDbClient, mockStepFunctionsClient, fixedClock);
    }

    @Test
    public void processStopEventShouldNotifyStepFunctionOfSuccessAndDeleteTokenWhenGivenSuccessEventId() {
        doNothing().when(mockLambdaLogger).log(anyString(), any());
        when(mockDynamoDbClient.retrieveTaskDetail(eq(TABLE), any())).thenReturn(Optional.of(new TaskDetail(TOKEN, false)));

        undertest.processStopEvent(mockLambdaLogger, TABLE, "task-key", TASK_ARN, DMS_TASK_SUCCESS_EVENT_ID);

        verify(mockStepFunctionsClient, times(1)).notifyStepFunctionSuccess(eq(TOKEN));
        verify(mockDynamoDbClient, times(1)).deleteToken(eq(TABLE), any());
        verifyNoMoreInteractions(mockStepFunctionsClient);
    }

    @Test
    public void processStopEventShouldNotifyStepFunctionOfFailureAndDeleteTokenWhenGivenFailedEventIdAndIgnoreFailureIsFalse() {
        boolean ignoreFailure = false;
        doNothing().when(mockLambdaLogger).log(anyString(), any());
        when(mockDynamoDbClient.retrieveTaskDetail(eq(TABLE), any())).thenReturn(Optional.of(new TaskDetail(TOKEN, ignoreFailure)));

        undertest.processStopEvent(mockLambdaLogger, TABLE, "task-key", TASK_ARN, DMS_TASK_FAILURE_EVENT_ID);

        verify(mockStepFunctionsClient, times(1)).notifyStepFunctionFailure(eq(TOKEN), anyString());
        verify(mockDynamoDbClient, times(1)).deleteToken(eq(TABLE), any());
        verifyNoMoreInteractions(mockStepFunctionsClient);
    }

    @Test
    public void processStopEventShouldNotifyStepFunctionOfSuccessAndDeleteTokenWhenGivenFailedEventIdAndIgnoreFailureIsTrue() {
        boolean ignoreFailure = true;
        doNothing().when(mockLambdaLogger).log(anyString(), any());
        when(mockDynamoDbClient.retrieveTaskDetail(eq(TABLE), any())).thenReturn(Optional.of(new TaskDetail(TOKEN, ignoreFailure)));

        undertest.processStopEvent(mockLambdaLogger, TABLE, "task-key", TASK_ARN, DMS_TASK_FAILURE_EVENT_ID);

        verify(mockStepFunctionsClient, times(1)).notifyStepFunctionSuccess(eq(TOKEN));
        verify(mockDynamoDbClient, times(1)).deleteToken(eq(TABLE), any());
        verifyNoMoreInteractions(mockStepFunctionsClient);
    }

    @Test
    public void processStopEventShouldNotNotifyStepFunctionsWhenTokenIsNotPresent() {
        doNothing().when(mockLambdaLogger).log(anyString(), any());
        when(mockDynamoDbClient.retrieveTaskDetail(eq(TABLE), any())).thenReturn(Optional.empty());

        assertThrows(Exception.class, () -> undertest.processStopEvent(mockLambdaLogger, TABLE, "task-key", TASK_ARN, DMS_TASK_SUCCESS_EVENT_ID));

        verify(mockDynamoDbClient, times(1)).retrieveTaskDetail(eq(TABLE), any());
        verifyNoInteractions(mockStepFunctionsClient);
        verifyNoMoreInteractions(mockDynamoDbClient);
    }

    @Test
    public void registerTaskDetailsShouldSaveTaskDetails() {
        boolean ignoreTaskFailure = false;

        undertest.registerTaskDetails(TOKEN, TASK_ARN, ignoreTaskFailure, TABLE, TOKEN_EXPIRY_DAYS);

        verify(mockDynamoDbClient, times(1))
                .saveTaskDetails(
                        eq(TABLE),
                        eq(TASK_ARN),
                        eq(TOKEN),
                        eq(ignoreTaskFailure),
                        eq(fixedDateTime.plusDays(TOKEN_EXPIRY_DAYS).toEpochSecond(ZoneOffset.UTC)),
                        eq(fixedDateTime.format(DateTimeFormatter.ISO_DATE_TIME))
                );
    }

}