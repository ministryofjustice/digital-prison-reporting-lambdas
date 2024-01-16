package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.s3.S3Client;
import uk.gov.justice.digital.clients.s3.S3Provider;
import uk.gov.justice.digital.lambda.S3DataDeletionLambda;

import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.Utils.*;
import static uk.gov.justice.digital.lambda.S3FileTransferLambda.*;

@ExtendWith(MockitoExtension.class)
public class S3DataDeletionLambdaIntegrationTest {

    @Mock
    private Context contextMock;
    @Mock
    private LambdaLogger mockLogger;
    @Mock
    private S3Provider mockS3Provider;
    @Mock
    private AmazonS3 mockS3;
    @Mock
    private ObjectListing mockObjectListing;

    @Captor
    ArgumentCaptor<String> sourceKeyCaptor, deleteKeyCaptor;

    @Captor
    ArgumentCaptor<ListObjectsRequest> listObjectsRequestCaptor;

    private final static String SOURCE_BUCKET = "source-bucket";

    private S3DataDeletionLambda underTest;

    @BeforeEach
    public void setup() {
        reset(contextMock, mockLogger, mockS3Provider, mockS3, mockObjectListing);

        when(contextMock.getLogger()).thenReturn(mockLogger);
        doNothing().when(mockLogger).log(anyString(), any());
        when(mockS3Provider.buildClient()).thenReturn(mockS3);

        underTest = new S3DataDeletionLambda(new S3Client(mockS3Provider));
    }

    @Test
    public void shouldDeleteParquetObjects() {
        Map<String, Object> event = createEvent();

        List<S3ObjectSummary> objectSummaries = createObjectSummaries(new Date());

        List<String> expectedKeys = new ArrayList<>();
        expectedKeys.add("1.parquet");
        expectedKeys.add("3.parquet");

        mockS3ObjectListing(objectSummaries);

        underTest.handleRequest(event, contextMock);

        verify(mockS3, times(expectedKeys.size())).deleteObject(
                eq(SOURCE_BUCKET),
                sourceKeyCaptor.capture()
        );

        assertThat(listObjectsRequestCaptor.getValue().getPrefix(), is(nullValue()));
        assertThat(sourceKeyCaptor.getAllValues(), containsInAnyOrder(expectedKeys.toArray()));

        verify(mockS3, times(expectedKeys.size())).deleteObject(eq(SOURCE_BUCKET), deleteKeyCaptor.capture());

        assertThat(deleteKeyCaptor.getAllValues(), containsInAnyOrder(expectedKeys.toArray()));
    }

    @Test
    public void shouldOnlyDeleteFilesBelongingToConfiguredTables() {
        String configKey = "test-config-key";
        String configBucket = "test-config-bucket";

        String configuredTable1 = "schema_1/table_1";
        String configuredTable2 = "schema_2/table_2";

        Map<String, Object> event = createEvent();
        event.put(CONFIG_OBJECT_KEY, Map.of(CONFIG_KEY, configKey, CONFIG_BUCKET, configBucket));

        Date lastModifiedDate = Date.from(new Date().toInstant());
        List<S3ObjectSummary> objectSummaries = createObjectSummaries(lastModifiedDate);

        mockS3ObjectListing(objectSummaries);
        when(mockS3.getObjectAsString(configBucket, CONFIG_PATH + configKey + CONFIG_FILE_SUFFIX))
                .thenReturn("{\"tables\": [\"" + configuredTable1 + "\", \"" + configuredTable2 + "\"]}");

        underTest.handleRequest(event, contextMock);

        assertThat(
                listObjectsRequestCaptor.getAllValues().stream().map(ListObjectsRequest::getPrefix).collect(Collectors.toList()),
                containsInAnyOrder(configuredTable1 + DELIMITER, configuredTable2 + DELIMITER)
        );

        verify(mockS3, times(4)).deleteObject(eq(SOURCE_BUCKET), deleteKeyCaptor.capture());
        assertThat(deleteKeyCaptor.getAllValues(), containsInAnyOrder("1.parquet", "3.parquet", "1.parquet", "3.parquet"));
    }

    private Map<String, Object> createEvent() {
        Map<String, Object> event = new HashMap<>();
        event.put(SOURCE_BUCKET_KEY, SOURCE_BUCKET);
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
        objectSummaries.add(createObjectSummary("3.parquet", lastModifiedDate));
        return objectSummaries;
    }
}
