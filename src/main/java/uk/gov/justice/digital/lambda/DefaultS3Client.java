package uk.gov.justice.digital.lambda;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class DefaultS3Client implements S3Client {

    public DefaultS3Client() {}

    @Override
    public AmazonS3 buildClient(String region) {
        return AmazonS3ClientBuilder.standard().withRegion(region).build();
    }
}
