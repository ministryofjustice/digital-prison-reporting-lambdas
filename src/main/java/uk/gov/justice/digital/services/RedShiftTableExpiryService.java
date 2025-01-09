package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;

import java.util.List;
import java.util.stream.IntStream;

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
            "AND COALESCE(json_extract_path_text(parameters, 'transient_lastDdlTime', TRUE), '0')::bigint < (EXTRACT(EPOCH FROM GETDATE()) - %d)";
    private static final int DROP_BATCH_SIZE = 500;
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
            logger.log(format("Found %d tables to remove", expiredTableNames.size()), LogLevel.INFO);

            if (!expiredTableNames.isEmpty()) {
                removeExternalTables(expiredTableNames, logger);
                logger.log(
                        format("Removed %d tables:\n%s", expiredTableNames.size(), join("\n", expiredTableNames)),
                        LogLevel.INFO
                );
            }
        } catch (Exception e) {
            logger.log(format("Failed to remove tables: %s", e.getMessage()), LogLevel.ERROR);
        }
    }

    private List<String> getExpiredExternalTableNames(LambdaLogger logger) {
        logger.log("Getting expired table names", LogLevel.INFO);

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

    private void removeExternalTables(List<String> tableNames, LambdaLogger logger) {
        List<String> dropStatements =  tableNames.stream()
                .map(tableName -> format(DROP_STATEMENT, tableName))
                        .collect(toList());

        int totalToDrop = dropStatements.size();


        IntStream.range(0, (totalToDrop + DROP_BATCH_SIZE-1) / DROP_BATCH_SIZE)
                .mapToObj(batchNum -> dropStatements.subList(batchNum * DROP_BATCH_SIZE, Math.min(totalToDrop, (batchNum+1) * DROP_BATCH_SIZE)))
                .parallel()
                .forEach(batch -> removeExternalTableBatch(batch, logger));

    }

    private void removeExternalTableBatch(List<String> dropStatements, LambdaLogger logger) {
        String singleStatement = String.join("\n", dropStatements);

        logger.log(format("Dropping tables:\n%s", singleStatement), LogLevel.INFO);

        var statementRequest = ExecuteStatementRequest.builder()
                .clusterIdentifier(clusterId)
                .database(databaseName)
                .secretArn(secretArn)
                .sql(singleStatement)
                .build();

        var response = dataClient.executeStatement(statementRequest);
        requestCompletesSuccessfully(response.id(), logger);
    }

    private boolean requestCompletesSuccessfully(String responseId, LambdaLogger logger) {
        var describeRequest = DescribeStatementRequest.builder().id(responseId).build();

        var describeResult = dataClient.describeStatement(describeRequest);

        while(!isFinished(describeResult)) {
            logger.log(format("Query status: %s", describeResult.status()), LogLevel.INFO);
            try {
                //noinspection BusyWait
                Thread.sleep(STATEMENT_STATUS_CHECK_DELAY_MILLIS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            describeResult = dataClient.describeStatement(describeRequest);
        }

        boolean success = describeResult.status().equals(StatusString.FINISHED);

        if (success) {
            logger.log("Query completed successfully", LogLevel.INFO);
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
