package uk.gov.justice.digital.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.s3.DefaultS3Provider;
import uk.gov.justice.digital.clients.s3.S3Client;
import uk.gov.justice.digital.clients.stepfunctions.DefaultStepFunctionsProvider;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.services.S3FileTransferService;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.gov.justice.digital.common.Utils.TASK_TOKEN_KEY;
import static uk.gov.justice.digital.common.Utils.getOptionalString;

/**
 * This Lambda moves parquet files from a source bucket to a destination bucket
 * using the following JSON event structure
 * <pre>
 * {
 *    "sourceBucket": "some-source-bucket",
 *    "destinationBucket": "some-destination-bucket",
 *    "retentionDays": "0",
 *    "sourceFolder": "some/folder"
 * }
 * </pre>
 * Only files with the lastModifiedDate older than the current date-time - retentionDays will be moved.
 * Files which failed to be copied will not be deleted.
 */
public class S3FileTransferLambda implements RequestHandler<Map<String, Object>, Void> {

    final static Long DEFAULT_RETENTION_DAYS = 0L;

    public final static String SOURCE_BUCKET_KEY = "sourceBucket";
    public final static String DESTINATION_BUCKET_KEY = "destinationBucket";
    // Optional folder(s) where files are located. Files outside specified folder are ignored
    public final static String SOURCE_FOLDER_KEY = "sourceFolder";
    // Optional field defaults to 0 (i.e. delete all files with modified date-time older than current date-time)
    public final static String RETENTION_DAYS_KEY = "retentionDays";

    private final S3FileTransferService service;

    @SuppressWarnings("unused")
    public S3FileTransferLambda() {
        S3Client s3Client = new S3Client(new DefaultS3Provider());
        StepFunctionsClient stepFunctionsClient = new StepFunctionsClient(new DefaultStepFunctionsProvider());
        Clock clock = Clock.systemUTC();
        this.service = new S3FileTransferService(s3Client, stepFunctionsClient, clock);
    }

    public S3FileTransferLambda(
            S3Client s3Client,
            StepFunctionsClient stepFunctionsClient,
            Clock clock
    ) {
        this.service = new S3FileTransferService(s3Client, stepFunctionsClient, clock);
    }

    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {

        LambdaLogger logger = context.getLogger();
        final String sourceBucket = (String) event.get(SOURCE_BUCKET_KEY);
        final String destinationBucket = (String) event.get(DESTINATION_BUCKET_KEY);
        final String sourceFolder = getOptionalString(event, SOURCE_FOLDER_KEY).orElse("");
        final Long retentionDays = getOptionalString(event, RETENTION_DAYS_KEY)
                .map(Long::parseLong)
                .orElse(DEFAULT_RETENTION_DAYS);

        // Optional task token. Present when triggered from step function. Absent when triggered from cloudwatch schedule
        final String token = (String) event.get(TASK_TOKEN_KEY);

        logger.log("Listing files in S3 source location: " + sourceBucket, LogLevel.INFO);
        List<String> objectKeys = service.listParquetFiles(sourceBucket, sourceFolder, retentionDays);

        logger.log(String.format("Moving S3 objects older than %d day(s) from %s to %s", retentionDays, sourceBucket, destinationBucket));
        Set<String> failedObjects = service.moveObjects(logger, objectKeys, sourceBucket, destinationBucket, token);

        if (failedObjects.isEmpty()) {
            logger.log(String.format("Successfully moved %d S3 files", objectKeys.size()), LogLevel.INFO);
        } else {
            logger.log("Not all S3 files were moved", LogLevel.WARN);
            System.exit(1);
        }
        return null;
    }

}
