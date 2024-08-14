package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;

import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RedShiftTableExpiryServiceTest {

    private static final String CLUSTER_ID = "CLUSTER_ID";
    private static final String DB_NAME = "DB_NAME";
    private static final String SECRET_ARN = "SECRET_ARN";
    private static final int TABLE_EXPIRY_SECONDS = 100;

    @Mock
    RedshiftDataClient dataClient;
    @Mock
    LambdaLogger mockLambdaLogger;

    private RedShiftTableExpiryService undertest;

    @BeforeEach
    public void setup() {
        undertest = new RedShiftTableExpiryService(dataClient, CLUSTER_ID, DB_NAME, SECRET_ARN, TABLE_EXPIRY_SECONDS);
    }

    @Test
    public void removeExpiredExternalTables_success_shouldCompleteSuccessfully() {
        String getTableId = "GET_TABLE_ID";
        String removeTableId = "REMOVE_TABLE_ID";
        String tableName = "TABLE_NAME";
        Collection<Collection<Field>> records = singletonList(singletonList(Field.builder().stringValue(tableName).build()));

        doNothing().when(mockLambdaLogger).log(anyString(), eq(LogLevel.INFO));

        // Get list of expired tables
        when(dataClient.executeStatement((ExecuteStatementRequest) any()))
                .thenReturn(ExecuteStatementResponse.builder().id(getTableId).build())
                .thenReturn(ExecuteStatementResponse.builder().id(removeTableId).build());
        when(dataClient.describeStatement(DescribeStatementRequest.builder().id(getTableId).build()))
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.STARTED).build())
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.FINISHED).build());
        when(dataClient.getStatementResult((GetStatementResultRequest) any()))
                .thenReturn(GetStatementResultResponse.builder().records(records).build());

        // Remove tables
        when(dataClient.describeStatement(DescribeStatementRequest.builder().id(removeTableId).build()))
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.STARTED).build())
                .thenReturn(DescribeStatementResponse.builder().status(StatusString.FINISHED).build());

        undertest.removeExpiredExternalTables(mockLambdaLogger);

        verify(dataClient, times(2)).executeStatement((ExecuteStatementRequest) any());
        verify(dataClient).getStatementResult((GetStatementResultRequest) any());
        verify(dataClient, times(2)).describeStatement(DescribeStatementRequest.builder().id(getTableId).build());
        verify(dataClient, times(2)).describeStatement(DescribeStatementRequest.builder().id(removeTableId).build());
    }
}
