package uk.gov.justice.digital.services;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import software.amazon.awssdk.services.redshiftdata.model.*;
import uk.gov.justice.digital.TableS3MetaData;
import uk.gov.justice.digital.clients.redshift.ExternalTableQueryExecutor;
import uk.gov.justice.digital.clients.s3.S3Client;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;

public class RedShiftTableExpiryService {

    private final int expirySeconds;
    private final S3Client s3Client;
    private final ExternalTableQueryExecutor queryExecutor;

    public RedShiftTableExpiryService(
            S3Client s3Client,
            ExternalTableQueryExecutor queryExecutor,
            int expirySeconds
    ) {
        this.queryExecutor = queryExecutor;
        this.expirySeconds = expirySeconds;
        this.s3Client = s3Client;
    }

    public void removeExpiredExternalTables(LambdaLogger logger) {
        try {
            logger.log("Requesting expired table names", LogLevel.INFO);
            ExecuteStatementResponse expiredTablesResponse = queryExecutor.startExpiredTablesQuery(expirySeconds);

            logger.log("Requesting invalid table names", LogLevel.INFO);
            ExecuteStatementResponse invalidTablesResponse = queryExecutor.startInvalidTablesQuery();

            var expiredTableNames = queryExecutor.getExpiredExternalTableNames(expiredTablesResponse, logger);
            logger.log(format("Found %d expired tables to remove", expiredTableNames.size()), LogLevel.INFO);

            List<ExecuteStatementResponse> finalResponses = new ArrayList<>();

            if (!expiredTableNames.isEmpty()) {
                finalResponses.addAll(queryExecutor.removeExternalTables(expiredTableNames, logger));
                logger.log(
                        format("Removed %d tables:\n%s", expiredTableNames.size(), join("\n", expiredTableNames)),
                        LogLevel.INFO
                );
            }

            finalResponses.addAll(processInvalidTables(invalidTablesResponse, logger));

            finalResponses.forEach(r -> queryExecutor.requestCompletesSuccessfully(r.id(), logger));
        } catch (Exception e) {
            logger.log(format("Failed to remove tables: %s", e.getMessage()), LogLevel.ERROR);
        }
    }

    private Collection<ExecuteStatementResponse> processInvalidTables(ExecuteStatementResponse invalidTablesResponse, LambdaLogger logger) {
        var invalidTables = queryExecutor.getInvalidTables(invalidTablesResponse, logger).stream()
                        .map(t -> new TableS3MetaData(t.tableName, t.s3Location, s3Client.getObjectCreatedDate(t.s3Location)))
                                .collect(toList());

        List<ExecuteStatementResponse> responses = new ArrayList<>();

        var removeTables = invalidTables.stream()
                .filter(t -> t.createdEpochDate == null
                        || (t.createdEpochDate + expirySeconds) <= Instant.now().toEpochMilli())
                .map(t -> t.tableName).collect(toList());
        if (!removeTables.isEmpty()) {
            responses.addAll(queryExecutor.removeExternalTables(removeTables, logger));
        }

        var updateTables = invalidTables.stream()
                .filter(t -> t.createdEpochDate != null
                        && (t.createdEpochDate + expirySeconds) > Instant.now().toEpochMilli())
                .collect(toList());
        if (!updateTables.isEmpty()) {
            responses.addAll(queryExecutor.updateTableCreationDates(updateTables, logger));
        }

        return responses;
    }
}
