package uk.gov.justice.digital.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.s3.DefaultS3Provider;
import uk.gov.justice.digital.clients.s3.S3Client;
import uk.gov.justice.digital.clients.stepfunctions.DefaultStepFunctionsProvider;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.common.ConfigSourceDetails;
import uk.gov.justice.digital.services.S3FileTransferService;

import java.time.Clock;
import java.util.*;

import static uk.gov.justice.digital.common.Utils.*;

/**
 * This Lambda moves parquet files from a source bucket to a destination bucket
 * using the following JSON event structure
 * <pre>
 * {
 *    "sourceBucket": "some-source-bucket",
 *    "destinationBucket": "some-destination-bucket",
 *    "retentionDays": "0",
 *    "config": {
 *      "key": "domain",
 *      "bucket": "some-config-bucket"
 *    }
 * }
 * </pre>
 * Only files with the lastModifiedDate older than the current date-time - retentionDays will be moved.
 * Files which failed to be copied will not be deleted.
 */
public class S3FileTransferLambda implements RequestHandler<Map<String, Object>, Void> {

    final static Long DEFAULT_RETENTION_DAYS = 0L;

    public final static String SOURCE_BUCKET_KEY = "sourceBucket";
    public final static String DESTINATION_BUCKET_KEY = "destinationBucket";

    // Optional config. When missing, all files will be archived.
    public final static String CONFIG_OBJECT_KEY = "config";

    // Config key which maps to a list of tables. Only files belonging to those tables will be archived.
    public final static String CONFIG_KEY = "key";
    // S3 bucket name where the configs are located
    public final static String CONFIG_BUCKET = "bucket";
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
        final String sourceBucket = getOrThrow(event, SOURCE_BUCKET_KEY, String.class);
        final String destinationBucket = getOrThrow(event, DESTINATION_BUCKET_KEY, String.class);
        final Long retentionDays = getOptionalString(event, RETENTION_DAYS_KEY)
                .map(Long::parseLong)
                .orElse(DEFAULT_RETENTION_DAYS);

        // Optional task token. Present when triggered from step function. Absent when triggered from cloudwatch schedule
        final String token = (String) event.get(TASK_TOKEN_KEY);
        final Map<String, String> config = getConfig(event, CONFIG_OBJECT_KEY);

        List<String> objectKeys = new ArrayList<>();
        if (config.isEmpty()) {
            // When no config is provided, all files in s3 bucket are archived
            logger.log("Listing files in S3 source location: " + sourceBucket, LogLevel.INFO);
            objectKeys.addAll(service.listParquetFiles(sourceBucket, retentionDays));
        } else {
            // When config is provided, only files belonging to the configured tables are archived
            String configBucket = getOrThrow(config, CONFIG_BUCKET, String.class);
            String configKey = getOrThrow(config, CONFIG_KEY, String.class);

            logger.log(String.format("Listing files in S3 source location %s for domain %s", sourceBucket, configKey), LogLevel.INFO);
            ConfigSourceDetails configDetails = new ConfigSourceDetails(configBucket, configKey);
            objectKeys.addAll(service.listParquetFilesForConfig(sourceBucket, configDetails, retentionDays));
        }

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
