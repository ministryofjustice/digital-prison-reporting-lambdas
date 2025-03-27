package uk.gov.justice.digital.clients.secretsmanager;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;

public class AWSSecretsManagerProvider {

    public AWSSecretsManager buildClient() {
        return AWSSecretsManagerClientBuilder.standard().build();
    }
}
