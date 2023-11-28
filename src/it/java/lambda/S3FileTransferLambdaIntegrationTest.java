package lambda;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.SendTaskSuccessRequest;
import com.github.stefanbirkner.systemlambda.SystemLambda;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.s3.S3Client;
import uk.gov.justice.digital.clients.s3.S3Provider;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsClient;
import uk.gov.justice.digital.clients.stepfunctions.StepFunctionsProvider;
import uk.gov.justice.digital.lambda.S3FileTransferLambda;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.core.Every.everyItem;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.Utils.TASK_TOKEN_KEY;
import static uk.gov.justice.digital.lambda.S3FileTransferLambda.*;
import static lambda.test.Fixture.TEST_TOKEN;
import static lambda.test.Fixture.fixedClock;

@ExtendWith(MockitoExtension.class)
public class S3FileTransferLambdaIntegrationTest {

    @Mock
    private Context contextMock;
    @Mock
    private LambdaLogger mockLogger;
    @Mock
    private S3Provider mockS3Provider;
    @Mock
    private StepFunctionsProvider mockStepFunctionsProvider;
    @Mock
    private AmazonS3 mockS3;
    @Mock
    private AWSStepFunctions mockStepFunctions;
    @Mock
    private ObjectListing mockObjectListing;

    @Captor
    ArgumentCaptor<String> copySourceKeyCaptor, copyDestinationKeyCaptor, deleteKeyCaptor;

    @Captor
    ArgumentCaptor<ListObjectsRequest> listObjectsRequestCaptor;

    @Captor
    ArgumentCaptor<SendTaskSuccessRequest> sendStepFunctionsSuccessRequestCapture;

    private final static String SOURCE_BUCKET = "source-bucket";
    private final static String DESTINATION_BUCKET = "destination-bucket";

    private static final LocalDateTime fixedDateTime = LocalDateTime.now(fixedClock);

    private S3FileTransferLambda underTest;

    @BeforeEach
    public void setup() {
        when(contextMock.getLogger()).thenReturn(mockLogger);
        doNothing().when(mockLogger).log(anyString(), any());
        when(mockS3Provider.buildClient()).thenReturn(mockS3);
        when(mockStepFunctionsProvider.buildClient()).thenReturn(mockStepFunctions);

        underTest = new S3FileTransferLambda(
                new S3Client(mockS3Provider),
                new StepFunctionsClient(mockStepFunctionsProvider),
                fixedClock
        );
    }

    @Test
    public void shouldMoveParquetObjectsAndNotifyStepFunctionWhenThereIsATaskToken() {
        Map<String, Object> event = createEvent();
        event.put(TASK_TOKEN_KEY, TEST_TOKEN);

        Date lastModifiedDate = Date.from(fixedDateTime.minusHours(2L).toInstant(ZoneOffset.UTC));
        List<S3ObjectSummary> objectSummaries = createObjectSummaries(lastModifiedDate);

        List<String> expectedKeys = new ArrayList<>();
        expectedKeys.add("1.parquet");
        expectedKeys.add("3.parquet");
        expectedKeys.add("folder1/4.parquet");
        expectedKeys.add("folder1/5.parquet");
        expectedKeys.add("folder1/folder2/6.parquet");
        expectedKeys.add("folder3/8.parquet");

        mockS3ObjectListing(objectSummaries);

        underTest.handleRequest(event, contextMock);

        verify(mockS3, times(expectedKeys.size())).copyObject(
                eq(SOURCE_BUCKET),
                copySourceKeyCaptor.capture(),
                eq(DESTINATION_BUCKET),
                copyDestinationKeyCaptor.capture()
        );

        assertThat(listObjectsRequestCaptor.getValue().getPrefix(), Matchers.equalTo(""));
        assertThat(copySourceKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));
        assertThat(copyDestinationKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));

        verify(mockS3, times(expectedKeys.size())).deleteObject(eq(SOURCE_BUCKET), deleteKeyCaptor.capture());
        verify(mockStepFunctions, times(1))
                .sendTaskSuccess(sendStepFunctionsSuccessRequestCapture.capture());

        assertThat(deleteKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));
    }

    @Test
    public void shouldNotNotifyStepFunctionWhenThereIsNoTaskToken() {
        Map<String, Object> event = createEvent();

        Date lastModifiedDate = Date.from(fixedDateTime.minusHours(2L).toInstant(ZoneOffset.UTC));
        List<S3ObjectSummary> objectSummaries = createObjectSummaries(lastModifiedDate);

        List<String> expectedKeys = new ArrayList<>();
        expectedKeys.add("1.parquet");
        expectedKeys.add("3.parquet");
        expectedKeys.add("folder1/4.parquet");
        expectedKeys.add("folder1/5.parquet");
        expectedKeys.add("folder1/folder2/6.parquet");
        expectedKeys.add("folder3/8.parquet");

        mockS3ObjectListing(objectSummaries);

        underTest.handleRequest(event, contextMock);

        verify(mockS3, times(expectedKeys.size())).copyObject(
                eq(SOURCE_BUCKET),
                copySourceKeyCaptor.capture(),
                eq(DESTINATION_BUCKET),
                copyDestinationKeyCaptor.capture()
        );

        assertThat(listObjectsRequestCaptor.getValue().getPrefix(), Matchers.equalTo(""));
        assertThat(copySourceKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));
        assertThat(copyDestinationKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));

        verify(mockS3, times(expectedKeys.size())).deleteObject(eq(SOURCE_BUCKET), deleteKeyCaptor.capture());
        verifyNoInteractions(mockStepFunctions);

        assertThat(deleteKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));
    }

    @Test
    public void shouldOnlyMoveFilesOlderThanRetentionDays() {
        String expectedKey = "3.parquet";

        Map<String, Object> event = createEvent();
        event.put(RETENTION_DAYS_KEY, "1");

        Date lastModifiedDate = Date.from(fixedDateTime.toInstant(ZoneOffset.UTC));
        Date lastModifiedDateOld = Date.from(fixedDateTime.minusDays(1).toInstant(ZoneOffset.UTC));

        List<S3ObjectSummary> objectSummaries = new ArrayList<>();
        objectSummaries.add(createObjectSummary("1.parquet", lastModifiedDate));
        objectSummaries.add(createObjectSummary(expectedKey, lastModifiedDateOld));

        mockS3ObjectListing(objectSummaries);

        underTest.handleRequest(event, contextMock);

        verify(mockS3, times(1)).copyObject(any(), eq(expectedKey), any(), eq(expectedKey));
        verify(mockS3, times(1)).deleteObject(any(), eq(expectedKey));
    }

    @Test
    public void shouldOnlyDeleteParquetObjectsCopiedSuccessfully() throws Exception {
        String expectedDeleteKey = "folder1/5.parquet";
        Map<String, Object> event = createEvent();

        Date lastModifiedDate = Date.from(LocalDateTime.now(fixedClock).minusHours(2L).toInstant(ZoneOffset.UTC));
        List<S3ObjectSummary> objectSummaries = createObjectSummaries(lastModifiedDate);

        mockS3ObjectListing(objectSummaries);

        when(mockS3.copyObject(any(), eq(expectedDeleteKey), any(), eq(expectedDeleteKey)))
                .thenReturn(new CopyObjectResult());

        when(mockS3.copyObject(any(), not(eq(expectedDeleteKey)), any(), not(eq(expectedDeleteKey))))
                .thenThrow(new AmazonServiceException("Copy error"));

        SystemLambda.catchSystemExit(() -> underTest.handleRequest(event, contextMock));

        verify(mockS3, times(1)).deleteObject(eq(SOURCE_BUCKET), deleteKeyCaptor.capture());

        assertThat(deleteKeyCaptor.getValue(), is(expectedDeleteKey));
    }

    @Test
    public void shouldApplyFolderPrefixWhenListingObjects() {
        String folder = "folder1/";

        Map<String, Object> event = createEvent();
        event.put(SOURCE_FOLDER_KEY, folder);

        Date lastModifiedDate = Date.from(fixedDateTime.minusHours(2L).toInstant(ZoneOffset.UTC));
        List<S3ObjectSummary> objectSummaries = createObjectSummaries(lastModifiedDate);

        mockS3ObjectListing(objectSummaries);

        underTest.handleRequest(event, contextMock);

        assertThat(listObjectsRequestCaptor.getValue().getPrefix(), Matchers.equalTo(folder));
    }

    private Map<String, Object> createEvent() {
        Map<String, Object> event = new HashMap<>();
        event.put(SOURCE_BUCKET_KEY, SOURCE_BUCKET);
        event.put(DESTINATION_BUCKET_KEY, DESTINATION_BUCKET);
        return event;
    }

    private S3ObjectSummary createObjectSummary(String key, Date lastModifiedDate) {
        S3ObjectSummary objectSummary = new S3ObjectSummary();
        objectSummary.setKey(key);
        objectSummary.setLastModified(lastModifiedDate);

        return objectSummary;
    }

    private void mockS3ObjectListing(List<S3ObjectSummary> objectSummaries) {
        when(mockS3.listObjects(listObjectsRequestCaptor.capture())).thenReturn(mockObjectListing);
        when(mockObjectListing.getObjectSummaries()).thenReturn(objectSummaries);
        when(mockObjectListing.getMarker()).thenReturn(null);
        when(mockObjectListing.isTruncated()).thenReturn(false);
    }

    private List<S3ObjectSummary> createObjectSummaries(Date lastModifiedDate) {
        List<S3ObjectSummary> objectSummaries = new ArrayList<>();
        objectSummaries.add(createObjectSummary("1.parquet", lastModifiedDate));
        objectSummaries.add(createObjectSummary("2.txt", lastModifiedDate));
        objectSummaries.add(createObjectSummary("3.parquet", lastModifiedDate));
        objectSummaries.add(createObjectSummary("folder1/4.parquet", lastModifiedDate));
        objectSummaries.add(createObjectSummary("folder1/5.parquet", lastModifiedDate));
        objectSummaries.add(createObjectSummary("folder1/folder2/6.parquet", lastModifiedDate));
        objectSummaries.add(createObjectSummary("folder1/folder2/7.txt", lastModifiedDate));
        objectSummaries.add(createObjectSummary("folder3/8.parquet", lastModifiedDate));
        return objectSummaries;
    }
}
