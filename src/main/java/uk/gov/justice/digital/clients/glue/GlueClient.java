package uk.gov.justice.digital.clients.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchStopJobRunRequest;

public class GlueClient {

    private final AWSGlue glueClient;

    public GlueClient(GlueProvider glueProvider) {
        this.glueClient = glueProvider.buildClient();
    }

    public void stopJob(String jobName) {
        BatchStopJobRunRequest request = new BatchStopJobRunRequest().withJobName(jobName);
        glueClient.batchStopJobRun(request);
    }
}
