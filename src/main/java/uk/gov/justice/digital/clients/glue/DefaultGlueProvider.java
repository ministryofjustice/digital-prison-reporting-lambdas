package uk.gov.justice.digital.clients.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

import static uk.gov.justice.digital.common.Utils.DEFAULT_DPR_REGION;

public class DefaultGlueProvider implements GlueProvider {
    @Override
    public AWSGlue buildClient() {
        return AWSGlueClientBuilder.standard().withRegion(DEFAULT_DPR_REGION).build();
    }
}
