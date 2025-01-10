package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.redshiftdata.model.*;
import uk.gov.justice.digital.TableS3Location;
import uk.gov.justice.digital.TableS3MetaData;
import uk.gov.justice.digital.clients.redshift.ExternalTableQueryExecutor;
import uk.gov.justice.digital.clients.s3.S3Client;

import java.time.Instant;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RedShiftTableExpiryServiceTest {

    private static final int TABLE_EXPIRY_SECONDS = 200;

    @Mock
    ExternalTableQueryExecutor queryExecutor;
    @Mock
    S3Client s3Client;
    @Mock
    LambdaLogger mockLambdaLogger;

    private RedShiftTableExpiryService underTest;

    @BeforeEach
    public void setup() {
        underTest = new RedShiftTableExpiryService(s3Client, queryExecutor, TABLE_EXPIRY_SECONDS);
    }

    @Test
    public void removeExpiredExternalTables_success_shouldCompleteSuccessfully() {
        String getExpiredTablesId = "GET_EXPIRED_TABLES_ID";
        String getInvalidTablesId = "GET_INVALID_TABLES_ID";
        String removeTableId = "REMOVE_TABLE_ID";
        String expiredTableName = "TABLE_NAME";

        doNothing().when(mockLambdaLogger).log(anyString(), eq(LogLevel.INFO));

        when(queryExecutor.startExpiredTablesQuery(anyInt()))
                .thenReturn(ExecuteStatementResponse.builder().id(getExpiredTablesId).build());
        when(queryExecutor.startInvalidTablesQuery())
                .thenReturn(ExecuteStatementResponse.builder().id(getInvalidTablesId).build());
        when(queryExecutor.getExpiredExternalTableNames(any(), any()))
                .thenReturn(singletonList(expiredTableName));
        when(queryExecutor.removeExternalTables(any(), any()))
                .thenReturn(singletonList(ExecuteStatementResponse.builder().id(removeTableId).build()));
        when(queryExecutor.getInvalidTables(any(), any()))
                .thenReturn(emptyList());

        underTest.removeExpiredExternalTables(mockLambdaLogger);

        verify(queryExecutor).startExpiredTablesQuery(TABLE_EXPIRY_SECONDS);
        verify(queryExecutor).startInvalidTablesQuery();
        verify(queryExecutor).getExpiredExternalTableNames(ExecuteStatementResponse.builder().id(getExpiredTablesId).build(), mockLambdaLogger);
        verify(queryExecutor).removeExternalTables(singletonList(expiredTableName), mockLambdaLogger);
        verify(queryExecutor).getInvalidTables(ExecuteStatementResponse.builder().id(getInvalidTablesId).build(), mockLambdaLogger);
        verify(queryExecutor, times(0)).updateTableCreationDates(singletonList(any()), any());
    }

    @Test
    public void removeExpiredExternalTables_largeQuantity_shouldBatchSuccessfully() {
        String getExpiredTablesId = "GET_EXPIRED_TABLES_ID";
        String getInvalidTablesId = "GET_INVALID_TABLES_ID";
        String removeTableId = "REMOVE_TABLE_ID";
        String expiredTableName = "TABLE_NAME";

        doNothing().when(mockLambdaLogger).log(anyString(), eq(LogLevel.INFO));

        when(queryExecutor.startExpiredTablesQuery(anyInt()))
                .thenReturn(ExecuteStatementResponse.builder().id(getExpiredTablesId).build());
        when(queryExecutor.startInvalidTablesQuery())
                .thenReturn(ExecuteStatementResponse.builder().id(getInvalidTablesId).build());
        when(queryExecutor.getExpiredExternalTableNames(any(), any()))
                .thenReturn(Collections.nCopies(501, expiredTableName));
        when(queryExecutor.removeExternalTables(any(), any()))
                .thenReturn(singletonList(ExecuteStatementResponse.builder().id(removeTableId).build()));
        when(queryExecutor.getInvalidTables(any(), any()))
                .thenReturn(emptyList());

        underTest.removeExpiredExternalTables(mockLambdaLogger);

        verify(queryExecutor).startExpiredTablesQuery(TABLE_EXPIRY_SECONDS);
        verify(queryExecutor).startInvalidTablesQuery();
        verify(queryExecutor).getExpiredExternalTableNames(ExecuteStatementResponse.builder().id(getExpiredTablesId).build(), mockLambdaLogger);
        verify(queryExecutor).removeExternalTables(Collections.nCopies(501, expiredTableName), mockLambdaLogger);
        verify(queryExecutor).getInvalidTables(ExecuteStatementResponse.builder().id(getInvalidTablesId).build(), mockLambdaLogger);
        verify(queryExecutor, times(0)).updateTableCreationDates(singletonList(any()), any());
    }

    @Test
    public void removeExpiredExternalTables_invalidTablesWithNoData_shouldBeRemoved() {
        String getExpiredTablesId = "GET_EXPIRED_TABLES_ID";
        String getInvalidTablesId = "GET_INVALID_TABLES_ID";
        String invalidTableName = "TABLE_NAME";
        String invalidTableLocation = "TABLE_LOCATION";

        doNothing().when(mockLambdaLogger).log(anyString(), eq(LogLevel.INFO));

        when(queryExecutor.startExpiredTablesQuery(anyInt()))
                .thenReturn(ExecuteStatementResponse.builder().id(getExpiredTablesId).build());
        when(queryExecutor.startInvalidTablesQuery())
                .thenReturn(ExecuteStatementResponse.builder().id(getInvalidTablesId).build());
        when(queryExecutor.getExpiredExternalTableNames(any(), any()))
                .thenReturn(emptyList());
        when(queryExecutor.getInvalidTables(any(), any()))
                .thenReturn(singletonList(new TableS3Location(invalidTableName, invalidTableLocation)));
        when(s3Client.getObjectCreatedDate(any()))
                .thenReturn(null);

        underTest.removeExpiredExternalTables(mockLambdaLogger);

        verify(queryExecutor).startExpiredTablesQuery(TABLE_EXPIRY_SECONDS);
        verify(queryExecutor).startInvalidTablesQuery();
        verify(queryExecutor).getExpiredExternalTableNames(ExecuteStatementResponse.builder().id(getExpiredTablesId).build(), mockLambdaLogger);
        verify(queryExecutor).getInvalidTables(ExecuteStatementResponse.builder().id(getInvalidTablesId).build(), mockLambdaLogger);
        verify(queryExecutor).removeExternalTables(singletonList(invalidTableName), mockLambdaLogger);
        verify(queryExecutor, times(0)).updateTableCreationDates(singletonList(any()), any());
        verify(s3Client).getObjectCreatedDate(invalidTableLocation);
    }

    @Test
    public void removeExpiredExternalTables_invalidTablesWithExpiredCreationDate_shouldBeRemoved() {
        String getExpiredTablesId = "GET_EXPIRED_TABLES_ID";
        String getInvalidTablesId = "GET_INVALID_TABLES_ID";
        String invalidTableName = "TABLE_NAME";
        String invalidTableLocation = "TABLE_LOCATION";
        long created = Instant.now().toEpochMilli() - (TABLE_EXPIRY_SECONDS + 1);

        doNothing().when(mockLambdaLogger).log(anyString(), eq(LogLevel.INFO));

        when(queryExecutor.startExpiredTablesQuery(anyInt()))
                .thenReturn(ExecuteStatementResponse.builder().id(getExpiredTablesId).build());
        when(queryExecutor.startInvalidTablesQuery())
                .thenReturn(ExecuteStatementResponse.builder().id(getInvalidTablesId).build());
        when(queryExecutor.getExpiredExternalTableNames(any(), any()))
                .thenReturn(emptyList());
        when(queryExecutor.getInvalidTables(any(), any()))
                .thenReturn(singletonList(new TableS3Location(invalidTableName, invalidTableLocation)));
        when(s3Client.getObjectCreatedDate(any()))
                .thenReturn(created);

        underTest.removeExpiredExternalTables(mockLambdaLogger);

        verify(queryExecutor).startExpiredTablesQuery(TABLE_EXPIRY_SECONDS);
        verify(queryExecutor).startInvalidTablesQuery();
        verify(queryExecutor).getExpiredExternalTableNames(ExecuteStatementResponse.builder().id(getExpiredTablesId).build(), mockLambdaLogger);
        verify(queryExecutor).getInvalidTables(ExecuteStatementResponse.builder().id(getInvalidTablesId).build(), mockLambdaLogger);
        verify(queryExecutor).removeExternalTables(singletonList(invalidTableName), mockLambdaLogger);
        verify(queryExecutor, times(0)).updateTableCreationDates(singletonList(any()), any());
        verify(s3Client).getObjectCreatedDate(invalidTableLocation);
    }

    @Test
    public void removeExpiredExternalTables_invalidTablesWithValidCreationDate_shouldBeUpdated() {
        String getExpiredTablesId = "GET_EXPIRED_TABLES_ID";
        String getInvalidTablesId = "GET_INVALID_TABLES_ID";
        String invalidTableName = "TABLE_NAME";
        String invalidTableLocation = "TABLE_LOCATION";
        long created = Instant.now().toEpochMilli();

        doNothing().when(mockLambdaLogger).log(anyString(), eq(LogLevel.INFO));

        when(queryExecutor.startExpiredTablesQuery(anyInt()))
                .thenReturn(ExecuteStatementResponse.builder().id(getExpiredTablesId).build());
        when(queryExecutor.startInvalidTablesQuery())
                .thenReturn(ExecuteStatementResponse.builder().id(getInvalidTablesId).build());
        when(queryExecutor.getExpiredExternalTableNames(any(), any()))
                .thenReturn(emptyList());
        when(queryExecutor.getInvalidTables(any(), any()))
                .thenReturn(singletonList(new TableS3Location(invalidTableName, invalidTableLocation)));
        when(s3Client.getObjectCreatedDate(any()))
                .thenReturn(created);

        underTest.removeExpiredExternalTables(mockLambdaLogger);

        verify(queryExecutor).startExpiredTablesQuery(TABLE_EXPIRY_SECONDS);
        verify(queryExecutor).startInvalidTablesQuery();
        verify(queryExecutor).getExpiredExternalTableNames(ExecuteStatementResponse.builder().id(getExpiredTablesId).build(), mockLambdaLogger);
        verify(queryExecutor).getInvalidTables(ExecuteStatementResponse.builder().id(getInvalidTablesId).build(), mockLambdaLogger);
        verify(queryExecutor, times(0)).removeExternalTables(any(), any());
        verify(queryExecutor).updateTableCreationDates(singletonList(new TableS3MetaData(invalidTableName, invalidTableLocation, created)), mockLambdaLogger);
        verify(s3Client).getObjectCreatedDate(invalidTableLocation);
    }
}
