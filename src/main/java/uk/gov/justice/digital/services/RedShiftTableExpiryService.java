package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class RedShiftTableExpiryService {

    private static final String DROP_STATEMENT = "DROP TABLE IF EXISTS reports.%s;";
    private static final String GET_TABLES_STATEMENT =
            "SELECT tablename " +
            "FROM SVV_EXTERNAL_TABLES " +
            "WHERE schemaname = 'reports' " +
            "AND json_extract_path_text(parameters, 'transient_lastDdlTime', TRUE) < (EXTRACT(EPOCH FROM GETDATE()) - %d)";
    public static final int STATEMENT_STATUS_CHECK_DELAY_MILLIS = 1000;


    private final RedshiftDataClient dataClient;
    private final String clusterId;
    private final String databaseName;
    private final String secretArn;
    private final int expirySeconds;

    public RedShiftTableExpiryService(
            RedshiftDataClient dataClient,
            String clusterId,
            String databaseName,
            String secretArn,
            int expirySeconds
    ) {
        this.dataClient = dataClient;
        this.clusterId = clusterId;
        this.databaseName = databaseName;
        this.secretArn = secretArn;
        this.expirySeconds = expirySeconds;
    }

    public void removeExpiredExternalTables(LambdaLogger logger) {
        try {
            var expiredTableNames = getExpiredExternalTableNames();
            logger.log(String.format("Found %d tables to remove", expiredTableNames.size()));

            removeExternalTables(expiredTableNames);
            logger.log(String.format("Removed %d tables", expiredTableNames.size()));
        } catch (Exception e) {
            logger.log(String.format("Failed to remove tables: %s", e.getMessage()), LogLevel.ERROR);
        }
    }

    private List<String> getExpiredExternalTableNames() throws InterruptedException {
        var request = ExecuteStatementRequest.builder()
            .clusterIdentifier(clusterId)
            .database(databaseName)
            .secretArn(secretArn)
                .sql(String.format(GET_TABLES_STATEMENT, expirySeconds))
            .build();

        var response = dataClient.executeStatement(request);

        if (requestCompletesSuccessfully(response.id())) {
            var resultRequest = GetStatementResultRequest.builder().id(response.id()).build();

            var resultResponse = dataClient.getStatementResult(resultRequest);

            if (resultResponse.hasRecords()) {
                return resultResponse
                        .records().stream()
                        .map(row -> row.get(0).stringValue())
                        .collect(toList());
            }
        }

        return emptyList();
    }

    private void removeExternalTables(List<String> tableNames) throws InterruptedException {
        List<String> dropStatements = tableNames.stream()
                .map(tableName -> String.format(DROP_STATEMENT, tableName))
                .collect(toList());

        var statementRequest = BatchExecuteStatementRequest.builder()
                .clusterIdentifier(clusterId)
                .database(databaseName)
                .secretArn(secretArn)
                .sqls(dropStatements)
                .build();

        var response = dataClient.batchExecuteStatement(statementRequest);
        requestCompletesSuccessfully(response.id());
    }

    private boolean requestCompletesSuccessfully(String responseId) throws InterruptedException {
        var describeRequest = DescribeStatementRequest.builder().id(responseId).build();

        var describeResult = dataClient.describeStatement(describeRequest);

        while(!isFinished(describeResult)) {
            //noinspection BusyWait
            Thread.sleep(STATEMENT_STATUS_CHECK_DELAY_MILLIS);

            describeResult = dataClient.describeStatement(describeRequest);
        }

        return describeResult.status().equals(StatusString.FINISHED);
    }

    private boolean isFinished(DescribeStatementResponse describeResult) {
        switch (describeResult.status()) {
            case FAILED:
            case FINISHED:
            case ABORTED:
                return true;

            default:
                return false;
        }
    }
}
