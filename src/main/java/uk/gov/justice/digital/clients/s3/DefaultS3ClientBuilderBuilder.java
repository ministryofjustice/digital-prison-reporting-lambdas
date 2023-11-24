package uk.gov.justice.digital.clients.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class DefaultS3ClientBuilderBuilder implements S3ClientBuilder {

    public DefaultS3ClientBuilderBuilder() {}

    @Override
    public AmazonS3 buildClient(String region) {
        return AmazonS3ClientBuilder.standard().withRegion(region).build();
    }
}
