package uk.gov.justice.digital.clients.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;


import static uk.gov.justice.digital.common.Utils.DEFAULT_DPR_REGION;

public class S3Client {

    private final AmazonS3 client;

    public S3Client() {
        this.client = AmazonS3ClientBuilder.standard().withRegion(DEFAULT_DPR_REGION).build();
    }

    public Long getObjectCreatedDate(String location) {
        var uri = new AmazonS3URI(location);

        if (client.doesObjectExist(uri.getBucket(), uri.getKey())) {
            var metadata = client.getObjectMetadata(uri.getBucket(), uri.getKey());
            return metadata.getLastModified().getTime();
        }

        return null;
    }
}
