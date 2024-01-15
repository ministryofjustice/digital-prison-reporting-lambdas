package uk.gov.justice.digital.services;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.gov.justice.digital.clients.s3.S3Client;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.common.ConfigSourceDetails;

import java.time.Clock;
import java.util.*;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.common.Utils.*;

public class S3FileService {

    private final S3Client s3Client;
    private final StepFunctionsClient stepFunctionsClient;
    private final Clock clock;

    public S3FileService(
            S3Client s3Client,
            StepFunctionsClient stepFunctionsClient,
            Clock clock
    ) {
        this.s3Client = s3Client;
        this.stepFunctionsClient = stepFunctionsClient;
        this.clock = clock;
    }

    public List<String> listParquetFiles(String bucket, Long retentionDays) {
        return s3Client.getObjectsOlderThan(bucket, FILE_EXTENSION, retentionDays, clock);
    }

    public List<String> listParquetFilesForConfig(String sourceBucket, ConfigSourceDetails configDetails, Long retentionDays) {
        Set<String> configuredTables = getConfiguredTables(configDetails);

        return configuredTables.stream()
                .flatMap(configuredTable -> listFilesForTable(sourceBucket, retentionDays, configuredTable).stream())
                .collect(Collectors.toList());
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
                    stepFunctionsClient.notifyStepFunctionSuccess(retrievedToken);
                }
        );

        return failedObjects;
    }

    private List<String> listFilesForTable(String sourceBucket, Long retentionDays, String configuredTable) {
        return s3Client.getObjectsOlderThan(
                sourceBucket,
                configuredTable + DELIMITER, FILE_EXTENSION,
                retentionDays,
                clock
        );
    }

    @SuppressWarnings({"unchecked"})
    private Set<String> getConfiguredTables(ConfigSourceDetails configDetails) {
        try {
            String configFileKey = CONFIG_PATH + configDetails.getConfigKey() + CONFIG_FILE_SUFFIX;
            String configString = s3Client.getObject(configFileKey, configDetails.getBucket());
            HashMap<String, ArrayList<String>> config = new ObjectMapper().readValue(configString, HashMap.class);
            return new HashSet<>(config.get("tables"));
        } catch (Exception e) {
            throw new RuntimeException("Exception when loading config", e);
        }
    }

    public Set<String> deleteObjects(LambdaLogger logger, List<String> objectKeys, String sourceBucket) {
        Set<String> failedObjects = new HashSet<>();

        for (String objectKey : objectKeys) {
            try {
                s3Client.deleteObject(objectKey, sourceBucket);
            } catch (AmazonServiceException e) {
                logger.log(String.format("Failed to delete S3 object %s: %s", objectKey, e.getErrorMessage()), LogLevel.WARN);
                failedObjects.add(objectKey);
            }
        }

        return failedObjects;
    }
}
