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
import uk.gov.justice.digital.services.S3FileService;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.gov.justice.digital.common.Utils.*;

/**
 * This Lambda deletes objects from a source bucket
 * using the following JSON event structure
 * <pre>
 * {
 *    "sourceBucket": "some-source-bucket",
 *    "config": {
 *      "key": "domain",
 *      "bucket": "some-config-bucket"
 *    }
 * }
 * </pre>
 */
public class S3DataDeletionLambda implements RequestHandler<Map<String, Object>, Void> {

    public final static String SOURCE_BUCKET_KEY = "sourceBucket";

    // Optional config. When missing, all files will be archived.
    public final static String CONFIG_OBJECT_KEY = "config";

    // Config key which maps to a list of tables. Only files belonging to those tables will be archived.
    public final static String CONFIG_KEY = "key";
    // S3 bucket name where the configs are located
    public final static String CONFIG_BUCKET = "bucket";
    // Optional field defaults to 0 (i.e. delete all files with modified date-time older than current date-time)

    private final S3FileService service;

    @SuppressWarnings("unused")
    public S3DataDeletionLambda() {
        S3Client s3Client = new S3Client(new DefaultS3Provider());
        this.service = new S3FileService(
                s3Client,
                new StepFunctionsClient(new DefaultStepFunctionsProvider()),
                Clock.systemUTC()
        );
    }

    public S3DataDeletionLambda(S3Client s3Client) {
        this.service = new S3FileService(
                s3Client,
                new StepFunctionsClient(new DefaultStepFunctionsProvider()),
                Clock.systemUTC()
        );
    }

    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {

        LambdaLogger logger = context.getLogger();
        final String sourceBucket = getOrThrow(event, SOURCE_BUCKET_KEY, String.class);

        final Map<String, String> config = getConfig(event, CONFIG_OBJECT_KEY);

        List<String> objectKeys = new ArrayList<>();
        if (config.isEmpty()) {
            // When no config is provided, all files in s3 bucket are archived
            logger.log("Listing files in S3 source location: " + sourceBucket, LogLevel.INFO);
            objectKeys.addAll(service.listParquetFiles(sourceBucket, 0L));
        } else {
            // When config is provided, only files belonging to the configured tables are archived
            String configBucket = getOrThrow(config, CONFIG_BUCKET, String.class);
            String configKey = getOrThrow(config, CONFIG_KEY, String.class);

            logger.log(String.format("Listing files in S3 source location %s for domain %s", sourceBucket, configKey), LogLevel.INFO);
            ConfigSourceDetails configDetails = new ConfigSourceDetails(configBucket, configKey);
            objectKeys.addAll(service.listParquetFilesForConfig(sourceBucket, configDetails, 0L));
        }

        logger.log(String.format("Deleting S3 objects from %s ", sourceBucket));
        Set<String> failedObjects = service.deleteObjects(logger, objectKeys, sourceBucket);

        if (failedObjects.isEmpty()) {
            logger.log(String.format("Successfully deleted %d S3 files", objectKeys.size()), LogLevel.INFO);
        } else {
            logger.log("Not all S3 files were deleted", LogLevel.WARN);
            System.exit(1);
        }
        return null;
    }

}
