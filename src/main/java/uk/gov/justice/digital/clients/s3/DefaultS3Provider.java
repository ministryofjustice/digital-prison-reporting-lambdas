package uk.gov.justice.digital.clients.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import static uk.gov.justice.digital.common.Utils.DEFAULT_DPR_REGION;

public class DefaultS3Provider implements S3Provider {
    @Override
    public AmazonS3 buildClient() {
        return AmazonS3ClientBuilder.standard().withRegion(DEFAULT_DPR_REGION).build();
    }
}
