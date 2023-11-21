package uk.gov.justice.digital.lambda;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * This Lambda moves parquet files from a source bucket to a destination bucket
 * using the following JSON event structure
 * <p>
 *  {
 *     "sourceBucket": "dpr-landing-zone-development",
 *     "destinationBucket": "dpr-raw-zone-development",
 *     "retentionDays": 0,
 *     "sourceFolder": "OMS_OWNER/MOVEMENTS"
 *   }
 * </p>
 * Only files with the lastModifiedDate older than the current date-time - retentionDays will be moved.
 * Files which failed to be copied will not be deleted
 */
public class S3FileTransferLambda implements RequestHandler<Map<String, String>, Void> {

    private final S3Client s3ClientBuilder;

    public S3FileTransferLambda() {
        this.s3ClientBuilder = new DefaultS3Client();
    }

    public S3FileTransferLambda(S3Client s3ClientBuilder) {
        this.s3ClientBuilder = s3ClientBuilder;
    }

    final static String DELIMITER = "/";
    final static String FILE_EXTENSION = ".parquet";
    final static Long DEFAULT_RETENTION_DAYS = 0L;
    final static String DEFAULT_REGION = "eu-west-2";

    final static String SOURCE_BUCKET_KEY = "sourceBucket";
    final static String DESTINATION_BUCKET_KEY = "destinationBucket";

    // Optional aws region. Defaults to eu-west-2
    final static String AWS_REGION_KEY = "awsRegion";
    // Optional folder(s) where files are located. Files outside specified folder are ignored
    final static String SOURCE_FOLDER_KEY = "sourceFolder";
    // Optional field defaults to 0 (i.e. delete all files with modified date-time older than current date-time)
    final static String RETENTION_DAYS_KEY = "retentionDays";

    @Override
    public Void handleRequest(Map<String, String> event, Context context) {

        LambdaLogger logger = context.getLogger();
        Set<String> failedObjects = new HashSet<>();

        final String sourceBucket = event.get(SOURCE_BUCKET_KEY);
        final String destinationBucket = event.get(DESTINATION_BUCKET_KEY);

        final String region = Optional.ofNullable(event.get(AWS_REGION_KEY)).orElse(DEFAULT_REGION);
        final String sourceFolder = Optional.ofNullable(event.get(SOURCE_FOLDER_KEY)).orElse("");

        final Long retentionDays = Optional
                .ofNullable(event.get(RETENTION_DAYS_KEY))
                .map(Long::parseLong)
                .orElse(DEFAULT_RETENTION_DAYS);

        final AmazonS3 s3 = s3ClientBuilder.buildClient(region);

        logger.log(String.format("Moving S3 objects older than %d days from %s to %s", retentionDays, sourceBucket, destinationBucket));

        logger.log("Listing files in S3 source location: " + sourceBucket);
        List<String> objectKeys = getObjectsList(s3, sourceBucket, sourceFolder, retentionDays);

        for (String objectKey : objectKeys) {

            try {
                logger.log("Copying " + objectKey);
                s3.copyObject(sourceBucket, objectKey, destinationBucket, objectKey);
                logger.log("Deleting " + objectKey);
                s3.deleteObject(sourceBucket, objectKey);
            } catch (AmazonServiceException e) {
                logger.log(String.format("Failed to move S3 object %s: %s", objectKey, e.getErrorMessage()));
                failedObjects.add(objectKey);
            }
        }

        if (failedObjects.isEmpty()) {
            logger.log(String.format("Successfully moved %d S3 files", objectKeys.size()));
        } else {
            logger.log("Not all S3 files were moved");
            System.exit(1);
        }
        return null;
    }

    protected static List<String> getObjectsList(AmazonS3 s3, String bucket, String folder, Long retentionDays) {
        if (!folder.isEmpty() && !folder.endsWith(DELIMITER)) {
            folder = folder + DELIMITER;
        }

        ZoneId utcZoneId = ZoneId.of("UTC");
        List<String> objectPaths = new LinkedList<>();
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucket).withPrefix(folder);
        LocalDateTime currentDate = LocalDateTime.now(utcZoneId);

        ObjectListing objectList;
        do {
            objectList = s3.listObjects(request);
            for (S3ObjectSummary summary : objectList.getObjectSummaries()) {

                LocalDateTime lastModifiedDate = summary.getLastModified().toInstant().atZone(utcZoneId).toLocalDateTime();
                boolean isBeforeRetentionPeriod = lastModifiedDate.isBefore(currentDate.minusDays(retentionDays));

                String summaryKey = summary.getKey();

                if (!summaryKey.endsWith(DELIMITER) && summaryKey.endsWith(FILE_EXTENSION) && isBeforeRetentionPeriod) {
                    objectPaths.add(summaryKey);
                }
            }
            request.setMarker(objectList.getMarker());
        } while (objectList.isTruncated());

        return objectPaths;
    }

}
