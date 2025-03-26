package uk.gov.justice.digital.clients.secretsmanager;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.fasterxml.jackson.databind.ObjectMapper;


public class SecretsManagerClient {

    private final AWSSecretsManager secretsClient;

    public SecretsManagerClient(AWSSecretsManagerProvider secretsClient) {
        this.secretsClient = secretsClient.buildClient();
    }

    public <T> T getSecret(String secretId, Class<T> type) {
        try {
            GetSecretValueRequest request = new GetSecretValueRequest().withSecretId(secretId);
            String secretValue = secretsClient.getSecretValue(request).getSecretString();
            return new ObjectMapper().readValue(secretValue, type);
        } catch (Exception ex) {
            throw new RuntimeException("Cannot retrieve secret " + secretId, ex);
        }
    }

}
