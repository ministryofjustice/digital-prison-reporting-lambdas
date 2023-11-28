package uk.gov.justice.digital.services;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.s3.S3Client;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.Utils.DELIMITER;
import static uk.gov.justice.digital.common.Utils.FILE_EXTENSION;
import static uk.gov.justice.digital.services.test.Fixture.containsTheSameElementsInOrderAs;
import static uk.gov.justice.digital.services.test.Fixture.fixedClock;

@ExtendWith(MockitoExtension.class)
class S3FileTransferServiceTest {

    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String DESTINATION_BUCKET = "destination-bucket";
    private static final String FOLDER = "test-folder";
    private static final String TOKEN = "token";
    private static final long RETENTION_DAYS = 2L;

    @Mock
    S3Client mockS3Client;
    @Mock
    StepFunctionsClient mockStepFunctionsClient;
    @Mock
    LambdaLogger mockLambdaLogger;

    private S3FileTransferService undertest;

    @BeforeEach
    public void setup() {
        undertest = new S3FileTransferService(mockS3Client, mockStepFunctionsClient, fixedClock);
    }

    @Test
    public void shouldReturnEmptyListWhenThereAreNoParquetFiles() {
        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());

        List<String> result = undertest.listParquetFiles(SOURCE_BUCKET, FOLDER, RETENTION_DAYS);

        assertThat(result, is(empty()));
    }

    @Test
    public void shouldListOfParquetFiles() {
        List<String> expected = new ArrayList<>();
        expected.add("file1.parquet");
        expected.add("file2.parquet");
        expected.add("file3.parquet");
        expected.add("file4.parquet");

        when(mockS3Client.getObjectsOlderThan(
                eq(SOURCE_BUCKET),
                eq(FOLDER + DELIMITER),
                eq(FILE_EXTENSION),
                eq(RETENTION_DAYS),
                eq(fixedClock))).thenReturn(expected);

        List<String> result = undertest.listParquetFiles(SOURCE_BUCKET, FOLDER, RETENTION_DAYS);

        assertThat(result, containsTheSameElementsInOrderAs(expected));
    }

    @Test
    public void shouldMoveObjectsAndNotifyStepFunction() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        doNothing().when(mockLambdaLogger).log(anyString(), any());

        Set<String> failedObjects = undertest.moveObjects(mockLambdaLogger, objectKeys, SOURCE_BUCKET, DESTINATION_BUCKET, TOKEN);

        verify(mockS3Client, times(objectKeys.size())).moveObject(any(), eq(SOURCE_BUCKET), eq(DESTINATION_BUCKET));
        verify(mockStepFunctionsClient, times(1)).notifyStepFunction(eq(TOKEN));

        assertThat(failedObjects, is(empty()));
    }

    @Test
    public void shouldReturnFailedObjects() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> expectedFailedObjects = new HashSet<>();
        expectedFailedObjects.add("file1.parquet");
        expectedFailedObjects.add("file2.parquet");
        expectedFailedObjects.add("file4.parquet");

        doNothing().when(mockLambdaLogger).log(anyString(), any());
        doThrow(new AmazonServiceException("failure")).when(mockS3Client).moveObject(any(), any(), any());
        doNothing().when(mockS3Client).moveObject(eq("file3.parquet"), any(), any());

        Set<String> failedObjects = undertest.moveObjects(mockLambdaLogger, objectKeys, SOURCE_BUCKET, DESTINATION_BUCKET, TOKEN);

        verify(mockStepFunctionsClient, times(1)).notifyStepFunction(eq("token"));

        assertEquals(failedObjects, expectedFailedObjects);
    }
}