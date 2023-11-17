package uk.gov.justice.digital.lambda;

import com.amazonaws.services.s3.AmazonS3;

public interface S3Client {
    AmazonS3 buildClient(String region);
}
