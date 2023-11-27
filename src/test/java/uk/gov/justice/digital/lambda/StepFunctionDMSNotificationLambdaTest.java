package uk.gov.justice.digital.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
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
import uk.gov.justice.digital.clients.dynamo.DynamoDbClientBuilder;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClientBuilder;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.lambda.StepFunctionDMSNotificationLambda.*;
import static uk.gov.justice.digital.lambda.StepFunctionDMSNotificationLambda.DEFAULT_REGION;

@ExtendWith(MockitoExtension.class)
public class StepFunctionDMSNotificationLambdaTest {

    @Mock
    private Context contextMock;
    @Mock
    private LambdaLogger mockLogger;
    @Mock
    private DynamoDbClientBuilder mockDynamoDbClientBuilder;
    @Mock
    private StepFunctionsClientBuilder mockStepFunctionsClientBuilder;
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

    private static final LocalDateTime fixedDateTime = LocalDateTime.now();

    private static final ZoneId utcZoneId = ZoneId.of("UTC");

    private final static String  TEST_TOKEN = "test-token";
    private final static String  TEST_TASK_ARN = "test-task-arn";

    private StepFunctionDMSNotificationLambda underTest;

    @BeforeEach
    public void setup() {
        when(contextMock.getLogger()).thenReturn(mockLogger);
        doNothing().when(mockLogger).log(anyString());
        when(mockDynamoDbClientBuilder.buildClient(eq(DEFAULT_REGION))).thenReturn(mockDynamoDb);

        underTest = new StepFunctionDMSNotificationLambda(
                mockDynamoDbClientBuilder,
                mockStepFunctionsClientBuilder,
                Clock.fixed(fixedDateTime.toInstant(ZoneOffset.UTC), utcZoneId)
        );
    }

    @Test
    public void shouldSaveTaskTokenToDynamoDb() {
        Map<String, Object> registerTokenEvent = createRegisterTaskTokenEvent();
        long expectedExpiry = fixedDateTime.plusDays(1).toEpochSecond(ZoneOffset.UTC);

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
        Map<String, Object> taskStoppedEvent = createDMSTaskStoppedEvent();
        GetItemResult getItemResult = new GetItemResult()
                .withItem(Map.of(TASK_TOKEN_KEY, new AttributeValue(TEST_TOKEN)));

        when(mockStepFunctionsClientBuilder.buildClient(eq(DEFAULT_REGION))).thenReturn(mockStepFunctions);
        when(mockDynamoDb.getItem(getItemRequestCapture.capture()))
                .thenReturn(getItemResult);

        underTest.handleRequest(taskStoppedEvent, contextMock);

        Map<String, AttributeValue> itemKey = getItemRequestCapture.getValue().getKey();
        assertThat(itemKey.get(REPLICATION_TASK_ARN_KEY).getS(), equalTo(TEST_TASK_ARN));

        verify(mockStepFunctions, times(1))
                .sendTaskSuccess(sendStepFunctionsSuccessRequestCapture.capture());

        SendTaskSuccessRequest actualSendTaskSuccessRequest = sendStepFunctionsSuccessRequestCapture.getValue();
        assertThat(actualSendTaskSuccessRequest.getTaskToken(), equalTo(TEST_TOKEN));
        assertThat(actualSendTaskSuccessRequest.getOutput(), equalTo("{}"));
    }

    @Test
    public void shouldNotSendRequestToStepFunctionsWhenThereIsNoTaskTokenInDynamoDb() {
        Map<String, Object> taskStoppedEvent = createDMSTaskStoppedEvent();
        GetItemResult emptyResult = new GetItemResult().withItem(Collections.emptyMap());

        when(mockDynamoDb.getItem(any())).thenReturn(emptyResult);

        underTest.handleRequest(taskStoppedEvent, contextMock);

        verify(mockStepFunctions, never()).sendTaskSuccess(any());
    }

    private Map<String, Object> createRegisterTaskTokenEvent() {
        Map<String, Object> event = new HashMap<>();
        event.put(TASK_TOKEN_KEY, TEST_TOKEN);
        event.put(REPLICATION_TASK_ARN_KEY, TEST_TASK_ARN);
        return event;
    }

    private Map<String, Object> createDMSTaskStoppedEvent() {
        ArrayList<String>resources = new ArrayList<>();
        resources.add(TEST_TASK_ARN);

        Map<String, Object> event = new HashMap<>();
        event.put(CLOUDWATCH_EVENT_RESOURCES_KEY, resources);
        return event;
    }
}
