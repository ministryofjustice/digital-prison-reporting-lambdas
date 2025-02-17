package uk.gov.justice.digital.clients.redshift;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;
import uk.gov.justice.digital.TableS3Location;

import java.util.List;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class ExternalTableQueryExecutor {
    private static final String DROP_STATEMENT = "DROP TABLE IF EXISTS reports.%s;";
    private static final String GET_EXPIRED_TABLES_STATEMENT =
            "SELECT tablename " +
                    "FROM SVV_EXTERNAL_TABLES " +
                    "WHERE schemaname = 'reports' " +
                    "AND json_extract_path_text(parameters, 'transient_lastDdlTime', TRUE)::bigint < (EXTRACT(EPOCH FROM GETDATE()) - %d)";
    private static final String GET_INVALID_TABLES_STATEMENT =
            "SELECT tablename, location " +
                    "FROM SVV_EXTERNAL_TABLES " +
                    "WHERE schemaname = 'reports' " +
                    "AND json_extract_path_text(parameters, 'transient_lastDdlTime', TRUE) IS NULL";

    private static final int BATCH_SIZE = 500;
    public static final int STATEMENT_STATUS_CHECK_DELAY_MILLIS = 1000;

    private final RedshiftDataClient dataClient;
    private final String clusterId;
    private final String databaseName;
    private final String secretArn;

    public ExternalTableQueryExecutor(RedshiftDataClient dataClient, String clusterId, String databaseName, String secretArn) {
        this.dataClient = dataClient;
        this.clusterId = clusterId;
        this.databaseName = databaseName;
        this.secretArn = secretArn;
    }

    public List<ExecuteStatementResponse> removeExternalTables(List<String> tableNames, LambdaLogger logger) {
        List<String> dropStatements =  tableNames.stream()
                .map(tableName -> format(DROP_STATEMENT, tableName))
                .collect(toList());

        return startQueries(logger, dropStatements);
    }

    public List<TableS3Location> getInvalidTables(ExecuteStatementResponse invalidTablesResponse, LambdaLogger logger) {
        if (requestCompletesSuccessfully(invalidTablesResponse.id(), logger)) {
            var resultRequest = GetStatementResultRequest.builder().id(invalidTablesResponse.id()).build();

            var resultResponse = dataClient.getStatementResult(resultRequest);

            if (resultResponse.hasRecords()) {
                return resultResponse
                        .records().stream()
                        .map(row -> new TableS3Location(row.get(0).stringValue(), row.get(1).stringValue()))
                        .collect(toList());
            }
        }

        return emptyList();
    }

    public List<String> getExpiredExternalTableNames(ExecuteStatementResponse expiredTablesResponse, LambdaLogger logger) {
        if (requestCompletesSuccessfully(expiredTablesResponse.id(), logger)) {
            var resultRequest = GetStatementResultRequest.builder().id(expiredTablesResponse.id()).build();

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

    public boolean requestCompletesSuccessfully(String responseId, LambdaLogger logger) {
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

    public ExecuteStatementResponse startExpiredTablesQuery(int expirySeconds) {
        return startQuery(format(GET_EXPIRED_TABLES_STATEMENT, expirySeconds));
    }

    public ExecuteStatementResponse startInvalidTablesQuery() {
        return startQuery(GET_INVALID_TABLES_STATEMENT);
    }

    private List<ExecuteStatementResponse> startQueries(LambdaLogger logger, List<String> statements) {
        int totalToDrop = statements.size();

        return IntStream.range(0, (totalToDrop + ExternalTableQueryExecutor.BATCH_SIZE - 1) / ExternalTableQueryExecutor.BATCH_SIZE)
                .mapToObj(batchNum -> statements.subList(batchNum * ExternalTableQueryExecutor.BATCH_SIZE, Math.min(totalToDrop, (batchNum + 1) * ExternalTableQueryExecutor.BATCH_SIZE)))
                .parallel()
                .map(batch -> startQueryBatch(batch, logger))
                .collect(toList());
    }

    private ExecuteStatementResponse startQueryBatch(List<String> statements, LambdaLogger logger) {
        String singleStatement = String.join("\n", statements);

        logger.log(format("Executing query batch:\n%s", singleStatement), LogLevel.INFO);

        return startQuery(singleStatement);
    }

    private ExecuteStatementResponse startQuery(String sql) {
        var request = ExecuteStatementRequest.builder()
                .clusterIdentifier(clusterId)
                .database(databaseName)
                .secretArn(secretArn)
                .sql(sql)
                .build();

        return dataClient.executeStatement(request);
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
