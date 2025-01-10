package uk.gov.justice.digital.clients.redshift;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExternalTableQueryExecutorTest {

    private static final String CLUSTER_ID = "CLUSTER_ID";
    private static final String DB_NAME = "DB_NAME";
    private static final String SECRET_ARN = "SECRET_ARN";

    @Mock
    RedshiftDataClient dataClient;
    @Mock
    LambdaLogger mockLambdaLogger;

    ExternalTableQueryExecutor target;

    @BeforeEach
    void setUp() {
        target = new ExternalTableQueryExecutor(dataClient, CLUSTER_ID, DB_NAME, SECRET_ARN);
    }

    @Test
    void removeExternalTables() {
        String tableName = "TABLE_NAME";
        String removeTableId = "REMOVE_TABLE_ID";
        var removeTablesResponse = ExecuteStatementResponse.builder().id(removeTableId).build();

        when(dataClient.executeStatement((ExecuteStatementRequest) any()))
                .thenReturn(removeTablesResponse);

        var responses = target.removeExternalTables(singletonList(tableName), mockLambdaLogger);

        assertEquals(responses.size(), 1);
        assertEquals(responses.get(0), removeTablesResponse);
    }

    @Test
    void getInvalidTables() {
        String getTablesId = "GET_TABLES_ID";
        String tableName = "TABLE_NAME";
        String tableLocation = "TABLE_LOCATION";
        var nameField = Field.builder().stringValue(tableName).build();
        var locationField = Field.builder().stringValue(tableLocation).build();
        var records = singletonList(asList(nameField, locationField));
        var statementResponse = ExecuteStatementResponse.builder().id(getTablesId).build();

        when(dataClient.describeStatement(DescribeStatementRequest.builder().id(getTablesId).build()))
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.STARTED).build())
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.FINISHED).build());
        when(dataClient.getStatementResult((GetStatementResultRequest) any()))
                .thenReturn(GetStatementResultResponse.builder().records(records).build());

        var invalidTables = target.getInvalidTables(statementResponse, mockLambdaLogger);

        verify(dataClient, times(2))
                .describeStatement(DescribeStatementRequest.builder().id(getTablesId).build());
        verify(dataClient)
                .getStatementResult(GetStatementResultRequest.builder().id(getTablesId).build());

        assertEquals(invalidTables.size(), 1);
        assertEquals(invalidTables.get(0).tableName, tableName);
        assertEquals(invalidTables.get(0).s3Location, tableLocation);
    }

    @Test
    void getExpiredExternalTableNames() {
        String getTablesId = "GET_TABLES_ID";
        String tableName = "TABLE_NAME";
        var nameField = Field.builder().stringValue(tableName).build();
        var records = singletonList(singletonList(nameField));
        var statementResponse = ExecuteStatementResponse.builder().id(getTablesId).build();

        when(dataClient.describeStatement(DescribeStatementRequest.builder().id(getTablesId).build()))
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.STARTED).build())
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.FINISHED).build());
        when(dataClient.getStatementResult((GetStatementResultRequest) any()))
                .thenReturn(GetStatementResultResponse.builder().records(records).build());

        var invalidTables = target.getExpiredExternalTableNames(statementResponse, mockLambdaLogger);

        verify(dataClient, times(2))
                .describeStatement(DescribeStatementRequest.builder().id(getTablesId).build());
        verify(dataClient)
                .getStatementResult(GetStatementResultRequest.builder().id(getTablesId).build());

        assertEquals(invalidTables.size(), 1);
        assertEquals(invalidTables.get(0), tableName);
    }

    @Test
    void requestCompletesSuccessfully_successful() {
        String responseId = "RESPONSE_ID";

        when(dataClient.describeStatement(DescribeStatementRequest.builder().id(responseId).build()))
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.STARTED).build())
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.FINISHED).build());

        var success = target.requestCompletesSuccessfully(responseId, mockLambdaLogger);

        verify(dataClient, times(2))
                .describeStatement(DescribeStatementRequest.builder().id(responseId).build());

        assertTrue(success);
    }

    @Test
    void requestCompletesSuccessfully_fails() {
        String responseId = "RESPONSE_ID";

        when(dataClient.describeStatement(DescribeStatementRequest.builder().id(responseId).build()))
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.STARTED).build())
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.FAILED).build());

        var success = target.requestCompletesSuccessfully(responseId, mockLambdaLogger);

        verify(dataClient, times(2))
                .describeStatement(DescribeStatementRequest.builder().id(responseId).build());

        assertFalse(success);
    }

    @Test
    void startExpiredTablesQuery() {
        String responseId = "RESPONSE_ID";
        int expirySeconds = 20;
        var response = ExecuteStatementResponse.builder().id(responseId).build();

        when(dataClient.executeStatement((ExecuteStatementRequest) any()))
                .thenReturn(response);

        var actualResponse = target.startExpiredTablesQuery(expirySeconds);

        assertEquals(actualResponse, response);
    }

    @Test
    void startInvalidTablesQuery() {
        String responseId = "RESPONSE_ID";
        var response = ExecuteStatementResponse.builder().id(responseId).build();

        when(dataClient.executeStatement((ExecuteStatementRequest) any()))
                .thenReturn(response);

        var actualResponse = target.startInvalidTablesQuery();

        assertEquals(actualResponse, response);
    }
}