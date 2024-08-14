package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;

import java.util.List;

import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class RedShiftTableExpiryService {

    private static final String DROP_STATEMENT = "DROP TABLE IF EXISTS reports.%s;";
    private static final String GET_TABLES_STATEMENT =
            "SELECT tablename " +
            "FROM SVV_EXTERNAL_TABLES " +
            "WHERE schemaname = 'reports' " +
            "AND json_extract_path_text(parameters, 'transient_lastDdlTime', TRUE) < (EXTRACT(EPOCH FROM GETDATE()) - %d)";
    private static final int MAX_BATCH_SIZE = 40;

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
            var expiredTableNames = getExpiredExternalTableNames(logger);
            logger.log(format("Found %d tables to remove", expiredTableNames.size()));

            removeExternalTables(expiredTableNames, logger);
            logger.log(format("Removed %d tables:\n%s", expiredTableNames.size(), join("\n", expiredTableNames)));
        } catch (Exception e) {
            logger.log(format("Failed to remove tables: %s", e.getMessage()), LogLevel.ERROR);
        }
    }

    private List<String> getExpiredExternalTableNames(LambdaLogger logger) throws InterruptedException {
        logger.log("Getting expired table names");

        var request = ExecuteStatementRequest.builder()
            .clusterIdentifier(clusterId)
            .database(databaseName)
            .secretArn(secretArn)
                .sql(format(GET_TABLES_STATEMENT, expirySeconds))
            .build();

        var response = dataClient.executeStatement(request);

        if (requestCompletesSuccessfully(response.id(), logger)) {
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

    private void removeExternalTables(List<String> tableNames, LambdaLogger logger) throws InterruptedException {
        List<String> dropStatements = tableNames.stream()
                .map(tableName -> format(DROP_STATEMENT, tableName))
                .collect(toList());

        int numberOfBatches = (dropStatements.size() + MAX_BATCH_SIZE - 1) / MAX_BATCH_SIZE;

        for (int i = 0; i < numberOfBatches; i++) {
            int batchStart = i * MAX_BATCH_SIZE;
            int batchEnd = min(dropStatements.size(), (i + 1) * MAX_BATCH_SIZE);

            logger.log(format("Removing tables %d to %d", batchStart + 1, batchEnd));

            removeExternalTableBatch(dropStatements.subList(batchStart, batchEnd), logger);
        }
    }

    private void removeExternalTableBatch(List<String> batch, LambdaLogger logger) throws InterruptedException {
        var statementRequest = BatchExecuteStatementRequest.builder()
                .clusterIdentifier(clusterId)
                .database(databaseName)
                .secretArn(secretArn)
                .sqls(batch)
                .build();

        var response = dataClient.batchExecuteStatement(statementRequest);
        requestCompletesSuccessfully(response.id(), logger);
    }

    private boolean requestCompletesSuccessfully(String responseId, LambdaLogger logger) throws InterruptedException {
        var describeRequest = DescribeStatementRequest.builder().id(responseId).build();

        var describeResult = dataClient.describeStatement(describeRequest);

        while(!isFinished(describeResult)) {
            logger.log(format("Query status: %s", describeResult.status()));
            //noinspection BusyWait
            Thread.sleep(STATEMENT_STATUS_CHECK_DELAY_MILLIS);

            describeResult = dataClient.describeStatement(describeRequest);
        }

        boolean success = describeResult.status().equals(StatusString.FINISHED);

        if (success) {
            logger.log("Query completed successfully");
        } else {
            logger.log(format("Query failed with status: %s - %s", describeResult.status(), describeResult.error()), LogLevel.ERROR);
        }

        return success;
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
