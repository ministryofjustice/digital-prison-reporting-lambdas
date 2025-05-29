package lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskFailureRequest;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.dynamo.DynamoDbClient;
import uk.gov.justice.digital.clients.dynamo.DynamoDbProvider;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsProvider;
import uk.gov.justice.digital.lambda.StepFunctionDMSNotificationLambda;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.reset;
import static uk.gov.justice.digital.clients.dynamo.DynamoDbClient.CREATED_AT_KEY;
import static uk.gov.justice.digital.clients.dynamo.DynamoDbClient.EXPIRE_AT_KEY;
import static lambda.test.Fixture.TEST_TOKEN;
import static lambda.test.Fixture.fixedClock;
import static uk.gov.justice.digital.common.Utils.REPLICATION_TASK_ARN_KEY;
import static uk.gov.justice.digital.common.Utils.TASK_TOKEN_KEY;
import static uk.gov.justice.digital.common.Utils.IGNORE_DMS_TASK_FAILURE_KEY;
import static uk.gov.justice.digital.common.Utils.TOKEN_EXPIRY_DAYS_KEY;
import static uk.gov.justice.digital.lambda.StepFunctionDMSNotificationLambda.CLOUDWATCH_EVENT_RESOURCES_KEY;
import static uk.gov.justice.digital.lambda.StepFunctionDMSNotificationLambda.CLOUDWATCH_EVENT_DETAIL_KEY;
import static uk.gov.justice.digital.lambda.StepFunctionDMSNotificationLambda.CLOUDWATCH_EVENT_ID_KEY;
import static uk.gov.justice.digital.services.StepFunctionDMSNotificationService.DMS_TASK_SUCCESS_EVENT_ID;
import static uk.gov.justice.digital.services.StepFunctionDMSNotificationService.DMS_TASK_FAILURE_EVENT_ID;

@ExtendWith(MockitoExtension.class)
public class StepFunctionDMSNotificationLambdaIntegrationTest {

    @Mock
    private Context contextMock;
    @Mock
    private LambdaLogger mockLogger;
    @Mock
    private DynamoDbProvider mockDynamoDbProvider;
    @Mock
    private StepFunctionsProvider mockStepFunctionsProvider;
    @Mock
    private AmazonDynamoDB mockDynamoDb;
    @Mock
    private AWSStepFunctions mockStepFunctions;

    @Captor
    ArgumentCaptor<PutItemRequest> putItemRequestCapture;
    @Captor
    ArgumentCaptor<GetItemRequest> getItemRequestCapture;
    @Captor
    ArgumentCaptor<SendTaskSuccessRequest> sendStepFunctionsSuccessRequestCapture;
    @Captor
    ArgumentCaptor<SendTaskFailureRequest> sendStepFunctionsFailureRequestCapture;

    private static final LocalDateTime fixedDateTime = LocalDateTime.now(fixedClock);

    private final static String TEST_TASK_ARN = "test-task-arn";
    private final static Long TEST_TOKEN_EXPIRY_DAYS = 2L;

    private StepFunctionDMSNotificationLambda underTest;

    @BeforeEach
    public void setup() {
        reset(contextMock, mockLogger, mockDynamoDb, mockDynamoDbProvider, mockStepFunctions, mockStepFunctionsProvider);
        when(contextMock.getLogger()).thenReturn(mockLogger);
        doNothing().when(mockLogger).log(anyString(), any());
        when(mockDynamoDbProvider.buildClient()).thenReturn(mockDynamoDb);
        when(mockStepFunctionsProvider.buildClient()).thenReturn(mockStepFunctions);

        underTest = new StepFunctionDMSNotificationLambda(
                new DynamoDbClient(mockDynamoDbProvider),
                new StepFunctionsClient(mockStepFunctionsProvider),
                fixedClock
        );
    }

    @Test
    public void shouldSaveTaskTokenToDynamoDb() {
        Map<String, Object> registerTokenEvent = createRegisterTaskTokenEvent();
        long expectedExpiry = fixedDateTime.plusDays(TEST_TOKEN_EXPIRY_DAYS).toEpochSecond(ZoneOffset.UTC);

        underTest.handleRequest(registerTokenEvent, contextMock);

        verify(mockDynamoDb, times(1)).putItem(putItemRequestCapture.capture());

        Map<String, AttributeValue> actualItem = putItemRequestCapture.getValue().getItem();
        assertThat(actualItem.get(REPLICATION_TASK_ARN_KEY).getS(), equalTo(TEST_TASK_ARN));
        assertThat(actualItem.get(TASK_TOKEN_KEY).getS(), equalTo(TEST_TOKEN));
        assertThat(actualItem.get(CREATED_AT_KEY).getS(), equalTo(fixedDateTime.format(DateTimeFormatter.ISO_DATE_TIME)));
        assertThat(actualItem.get(EXPIRE_AT_KEY).getN(), equalTo(String.valueOf(expectedExpiry)));
    }

    @Test
    public void shouldSendSuccessRequestToStepFunctionsUsingTaskTokenRetrievedFromDynamoDb() {
        Map<String, Object> taskSuccessfulStoppedEvent = createDMSTaskSuccessfulStoppageEvent();
        GetItemResult getItemResult = new GetItemResult()
                .withItem(Map.of(TASK_TOKEN_KEY, new AttributeValue(TEST_TOKEN)));

        when(mockDynamoDb.getItem(getItemRequestCapture.capture())).thenReturn(getItemResult);

        underTest.handleRequest(taskSuccessfulStoppedEvent, contextMock);

        Map<String, AttributeValue> itemKey = getItemRequestCapture.getValue().getKey();
        assertThat(itemKey.get(REPLICATION_TASK_ARN_KEY).getS(), equalTo(TEST_TASK_ARN));

        verify(mockStepFunctions, times(1))
                .sendTaskSuccess(sendStepFunctionsSuccessRequestCapture.capture());

        SendTaskSuccessRequest actualSendTaskSuccessRequest = sendStepFunctionsSuccessRequestCapture.getValue();
        assertThat(actualSendTaskSuccessRequest.getTaskToken(), equalTo(TEST_TOKEN));
        assertThat(actualSendTaskSuccessRequest.getOutput(), equalTo("{}"));
    }

    @Test
    public void shouldSendFailureRequestToStepFunctionsUsingTaskTokenRetrievedFromDynamoDbWhenDmsTaskFailureEventIsReceived() {
        Map<String, Object> taskFailedEvent = createDMSTaskFailedStoppageEvent();
        GetItemResult getItemResult = new GetItemResult()
                .withItem(Map.of(TASK_TOKEN_KEY, new AttributeValue(TEST_TOKEN)));

        when(mockDynamoDb.getItem(getItemRequestCapture.capture())).thenReturn(getItemResult);

        underTest.handleRequest(taskFailedEvent, contextMock);

        Map<String, AttributeValue> itemKey = getItemRequestCapture.getValue().getKey();
        assertThat(itemKey.get(REPLICATION_TASK_ARN_KEY).getS(), equalTo(TEST_TASK_ARN));

        verify(mockStepFunctions, times(1))
                .sendTaskFailure(sendStepFunctionsFailureRequestCapture.capture());

        SendTaskFailureRequest actualSendTaskFailureRequest = sendStepFunctionsFailureRequestCapture.getValue();
        assertThat(actualSendTaskFailureRequest.getTaskToken(), equalTo(TEST_TOKEN));
        assertThat(actualSendTaskFailureRequest.getError(), containsString(TEST_TASK_ARN));
    }

    @Test
    public void shouldSendFailureRequestToStepFunctionsUsingTaskTokenRetrievedFromDynamoDbWhenDmsTaskFailureEventIsReceivedAndTheIgnoreDmsTaskFailureFlagIsNotSet() {
        Map<String, Object> taskFailedEvent = createDMSTaskFailedStoppageEvent();
        Map<String, AttributeValue> attributes = Map.of(
                TASK_TOKEN_KEY, new AttributeValue(TEST_TOKEN),
                IGNORE_DMS_TASK_FAILURE_KEY, new AttributeValue().withBOOL(false)
        );
        GetItemResult getItemResult = new GetItemResult().withItem(attributes);

        when(mockDynamoDb.getItem(getItemRequestCapture.capture())).thenReturn(getItemResult);

        underTest.handleRequest(taskFailedEvent, contextMock);

        Map<String, AttributeValue> itemKey = getItemRequestCapture.getValue().getKey();
        assertThat(itemKey.get(REPLICATION_TASK_ARN_KEY).getS(), equalTo(TEST_TASK_ARN));

        verify(mockStepFunctions, times(1))
                .sendTaskFailure(sendStepFunctionsFailureRequestCapture.capture());

        SendTaskFailureRequest actualSendTaskFailureRequest = sendStepFunctionsFailureRequestCapture.getValue();
        assertThat(actualSendTaskFailureRequest.getTaskToken(), equalTo(TEST_TOKEN));
        assertThat(actualSendTaskFailureRequest.getError(), containsString(TEST_TASK_ARN));
    }

    @Test
    public void shouldOnlySendSuccessRequestWhenDmsTaskFailureEventIsReceivedAndTheIgnoreDmsTaskFailureFlagIsSet() {
        Map<String, Object> taskFailedEvent = createDMSTaskFailedStoppageEvent();

        Map<String, AttributeValue> attributes = Map.of(
                TASK_TOKEN_KEY, new AttributeValue(TEST_TOKEN),
                IGNORE_DMS_TASK_FAILURE_KEY, new AttributeValue().withBOOL(true)
        );

        GetItemResult getItemResult = new GetItemResult().withItem(attributes);

        when(mockDynamoDb.getItem(getItemRequestCapture.capture())).thenReturn(getItemResult);

        underTest.handleRequest(taskFailedEvent, contextMock);

        Map<String, AttributeValue> itemKey = getItemRequestCapture.getValue().getKey();
        assertThat(itemKey.get(REPLICATION_TASK_ARN_KEY).getS(), equalTo(TEST_TASK_ARN));

        verify(mockStepFunctions, times(1))
                .sendTaskSuccess(sendStepFunctionsSuccessRequestCapture.capture());

        SendTaskSuccessRequest actualSendTaskSuccessRequest = sendStepFunctionsSuccessRequestCapture.getValue();
        assertThat(actualSendTaskSuccessRequest.getTaskToken(), equalTo(TEST_TOKEN));
        assertThat(actualSendTaskSuccessRequest.getOutput(), equalTo("{}"));

        verifyNoMoreInteractions(mockStepFunctions);
    }

    @Test
    public void shouldFailWhenThereIsNoTaskTokenInDynamoDb() {
        Map<String, Object> taskStoppedEvent = createDMSTaskSuccessfulStoppageEvent();
        GetItemResult emptyResult = new GetItemResult().withItem(Collections.emptyMap());

        when(mockDynamoDb.getItem(any())).thenReturn(emptyResult);

        assertThrows(Exception.class, () -> underTest.handleRequest(taskStoppedEvent, contextMock));
    }

    private Map<String, Object> createRegisterTaskTokenEvent() {
        Map<String, Object> event = new HashMap<>();
        event.put(TASK_TOKEN_KEY, TEST_TOKEN);
        event.put(REPLICATION_TASK_ARN_KEY, TEST_TASK_ARN);
        event.put(TOKEN_EXPIRY_DAYS_KEY, TEST_TOKEN_EXPIRY_DAYS.toString());
        return event;
    }

    private Map<String, Object> createDMSTaskSuccessfulStoppageEvent() {
        return createDMSTaskStoppageEvent(DMS_TASK_SUCCESS_EVENT_ID);
    }

    private Map<String, Object> createDMSTaskFailedStoppageEvent() {
        return createDMSTaskStoppageEvent(DMS_TASK_FAILURE_EVENT_ID);
    }

    private static Map<String, Object> createDMSTaskStoppageEvent(String eventId) {
        ArrayList<String> resources = new ArrayList<>();
        resources.add(TEST_TASK_ARN);

        LinkedHashMap<String, Object> detail = new LinkedHashMap<>();
        detail.put(CLOUDWATCH_EVENT_ID_KEY, eventId);

        Map<String, Object> event = new HashMap<>();

        event.put(CLOUDWATCH_EVENT_RESOURCES_KEY, resources);
        event.put(CLOUDWATCH_EVENT_DETAIL_KEY, detail);
        return event;
    }
}
