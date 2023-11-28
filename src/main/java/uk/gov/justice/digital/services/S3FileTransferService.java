package uk.gov.justice.digital.services;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.s3.S3Client;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;

import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static uk.gov.justice.digital.common.Utils.DELIMITER;
import static uk.gov.justice.digital.common.Utils.FILE_EXTENSION;

public class S3FileTransferService {

    private final S3Client s3Client;
    private final StepFunctionsClient stepFunctionsClient;
    private final Clock clock;

    public S3FileTransferService(
            S3Client s3Client,
            StepFunctionsClient stepFunctionsClient,
            Clock clock
    ) {
        this.s3Client = s3Client;
        this.stepFunctionsClient = stepFunctionsClient;
        this.clock = clock;
    }

    public List<String> listParquetFiles(String bucket, String folder, Long retentionDays) {
        if (!folder.isEmpty() && !folder.endsWith(DELIMITER)) {
            folder = folder + DELIMITER;
        }

        return s3Client.getObjectsOlderThan(bucket, folder, FILE_EXTENSION, retentionDays, clock);
    }

    public Set<String> moveObjects(
            LambdaLogger logger,
            List<String> objectKeys,
            String sourceBucket,
            String destinationBucket,
            String token
    ) {
        Set<String> failedObjects = new HashSet<>();

        for (String objectKey : objectKeys) {
            try {
                s3Client.moveObject(objectKey, sourceBucket, destinationBucket);
            } catch (AmazonServiceException e) {
                logger.log(String.format("Failed to move S3 object %s: %s", objectKey, e.getErrorMessage()), LogLevel.WARN);
                failedObjects.add(objectKey);
            }
        }

        Optional.ofNullable(token).ifPresent(retrievedToken -> {
                    logger.log(String.format("Notifying step functions of success using token %s", retrievedToken), LogLevel.INFO);
                    stepFunctionsClient.notifyStepFunction(retrievedToken);
                }
        );

        return failedObjects;
    }
}
