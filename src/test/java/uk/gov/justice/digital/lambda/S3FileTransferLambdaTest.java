package uk.gov.justice.digital.lambda;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.stefanbirkner.systemlambda.SystemLambda;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.s3.S3ClientBuilder;

import java.time.LocalDateTime;
import java.time.ZoneId;
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
import static uk.gov.justice.digital.lambda.S3FileTransferLambda.*;

@ExtendWith(MockitoExtension.class)
public class S3FileTransferLambdaTest {

    @Mock
    private Context contextMock;
    @Mock
    private LambdaLogger mockLogger;
    @Mock
    private S3ClientBuilder mockS3ClientBuilder;
    @Mock
    private AmazonS3 mockS3;
    @Mock
    private ObjectListing mockObjectListing;

    @Captor
    ArgumentCaptor<String> copySourceKeyCaptor, copyDestinationKeyCaptor, deleteKeyCaptor;

    @Captor
    ArgumentCaptor<ListObjectsRequest> requestCaptor;

    ZoneId utcZoneId = ZoneId.of("UTC");

    private final static String SOURCE_BUCKET = "source-bucket";
    private final static String DESTINATION_BUCKET = "destination-bucket";

    private S3FileTransferLambda underTest;

    @BeforeEach
    public void setup() {
        when(contextMock.getLogger()).thenReturn(mockLogger);
        doNothing().when(mockLogger).log(anyString());
        when(mockS3ClientBuilder.buildClient(eq(DEFAULT_REGION))).thenReturn(mockS3);

        underTest = new S3FileTransferLambda(mockS3ClientBuilder);
    }

    @Test
    public void shouldMoveParquetObjectsFromSourceToDestinationBucket() {
        Map<String, String> event = createEvent();

        Date lastModifiedDate = Date.from(LocalDateTime.now(utcZoneId).minusHours(2L).toInstant(ZoneOffset.UTC));
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

        assertThat(requestCaptor.getValue().getPrefix(), Matchers.equalTo(""));
        assertThat(copySourceKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));
        assertThat(copyDestinationKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));

        verify(mockS3, times(expectedKeys.size())).deleteObject(eq(SOURCE_BUCKET), deleteKeyCaptor.capture());

        assertThat(deleteKeyCaptor.getAllValues(), everyItem(is(in(expectedKeys))));
    }

    @Test
    public void shouldOnlyMoveFilesOlderThanRetentionDays() {
        String expectedKey = "3.parquet";

        Map<String, String> event = createEvent();
        event.put(RETENTION_DAYS_KEY, "1");

        LocalDateTime now = LocalDateTime.now(utcZoneId);

        Date lastModifiedDate = Date.from(now.toInstant(ZoneOffset.UTC));
        Date lastModifiedDateOld = Date.from(now.minusDays(1).toInstant(ZoneOffset.UTC));

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
        Map<String, String> event = createEvent();

        Date lastModifiedDate = Date.from(LocalDateTime.now(utcZoneId).minusHours(2L).toInstant(ZoneOffset.UTC));
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

        Map<String, String> event = createEvent();
        event.put(SOURCE_FOLDER_KEY, folder);

        Date lastModifiedDate = Date.from(LocalDateTime.now(utcZoneId).minusHours(2L).toInstant(ZoneOffset.UTC));
        List<S3ObjectSummary> objectSummaries = createObjectSummaries(lastModifiedDate);

        mockS3ObjectListing(objectSummaries);

        underTest.handleRequest(event, contextMock);

        assertThat(requestCaptor.getValue().getPrefix(), Matchers.equalTo(folder));
    }

    private Map<String, String> createEvent() {
        Map<String, String> event = new HashMap<>();
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
        when(mockS3.listObjects(requestCaptor.capture())).thenReturn(mockObjectListing);
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
