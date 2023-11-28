package uk.gov.justice.digital.clients.s3;

import com.amazonaws.services.s3.AmazonS3;

public interface S3Provider {
    AmazonS3 buildClient();
}
