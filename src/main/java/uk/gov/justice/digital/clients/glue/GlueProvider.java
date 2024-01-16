package uk.gov.justice.digital.clients.glue;

import com.amazonaws.services.glue.AWSGlue;

public interface GlueProvider {
    AWSGlue buildClient();
}
