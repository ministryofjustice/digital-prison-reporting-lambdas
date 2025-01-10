package uk.gov.justice.digital.clients.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;


import java.util.Date;

import static java.lang.String.format;
import static uk.gov.justice.digital.common.Utils.DEFAULT_DPR_REGION;

public class S3Client {

    private final AmazonS3 client;

    public S3Client() {
        this.client = AmazonS3ClientBuilder.standard().withRegion(DEFAULT_DPR_REGION).build();
    }

    public Long getEarliestObjectCreatedDate(String folderLocation) {
        var uri = new AmazonS3URI(folderLocation);

        var listRequest = new ListObjectsRequest()
                .withBucketName(uri.getBucket())
                .withPrefix(format("%s/", uri.getKey()));

        return client.listObjects(listRequest)
                .getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getLastModified)
                .sorted()
                .findFirst()
                .map(Date::getTime)
                .orElse(null);
    }
}
