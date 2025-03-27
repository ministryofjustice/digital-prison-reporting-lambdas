package uk.gov.justice.digital.clients.secretsmanager;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.services.postgrestickle.HeartBeatEndpointDetails;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SecretsManagerClientTest {

    private static final String SECRET_JSON_CONTENTS = "{\"db_name\":\"postgres\",\"heartbeat_endpoint\":\"somedb.abc.eu-west-2.rds.amazonaws.com\",\"password\":\"123\",\"port\":\"5432\",\"username\":\"some_user\"}\n";
    private static final String SECRET_JSON_CONTENTS_WITH_EXTRA_FIELD = "{\"extra_field\":\"some content\",\"db_name\":\"postgres\",\"heartbeat_endpoint\":\"somedb.abc.eu-west-2.rds.amazonaws.com\",\"password\":\"123\",\"port\":\"5432\",\"username\":\"some_user\"}\n";

    @Mock
    private AWSSecretsManagerProvider secretsManagerProvider;
    @Mock
    private AWSSecretsManager internalAwsClient;
    @Mock
    private GetSecretValueResult getSecretValueResult;

    private SecretsManagerClient underTest;

    @BeforeEach
    void setUp() {
        when(secretsManagerProvider.buildClient()).thenReturn(internalAwsClient);
        underTest = new SecretsManagerClient(secretsManagerProvider);
    }

    @Test
    void shouldGetAWellFormedSecret() {
        when(internalAwsClient.getSecretValue(any())).thenReturn(getSecretValueResult);
        when(getSecretValueResult.getSecretString()).thenReturn(SECRET_JSON_CONTENTS);

        HeartBeatEndpointDetails result = underTest.getSecret("mysecret", HeartBeatEndpointDetails.class);
        HeartBeatEndpointDetails expected = new HeartBeatEndpointDetails("some_user", "123", "somedb.abc.eu-west-2.rds.amazonaws.com", "postgres", 5432);
        assertEquals(expected, result);
    }

    @Test
    void shouldGetAWellFormedSecretWithUnrecognisedField() {
        when(internalAwsClient.getSecretValue(any())).thenReturn(getSecretValueResult);
        when(getSecretValueResult.getSecretString()).thenReturn(SECRET_JSON_CONTENTS_WITH_EXTRA_FIELD);

        HeartBeatEndpointDetails result = underTest.getSecret("mysecret", HeartBeatEndpointDetails.class);
        HeartBeatEndpointDetails expected = new HeartBeatEndpointDetails("some_user", "123", "somedb.abc.eu-west-2.rds.amazonaws.com", "postgres", 5432);
        assertEquals(expected, result);
    }

    @Test
    void shouldThrowForBadlyFormedSecret() {
        when(internalAwsClient.getSecretValue(any())).thenReturn(getSecretValueResult);
        when(getSecretValueResult.getSecretString()).thenReturn("{");

        assertThrows(RuntimeException.class, () -> underTest.getSecret("mysecret", HeartBeatEndpointDetails.class));
    }
}