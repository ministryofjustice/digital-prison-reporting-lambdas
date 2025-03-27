package uk.gov.justice.digital.services.postgrestickle;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.clients.jdbc.JdbcClient;
import uk.gov.justice.digital.clients.jdbc.PostgresJdbcClientProvider;
import uk.gov.justice.digital.clients.secretsmanager.SecretsManagerClient;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostgresTickleServiceTest {

    private static final String SECRET_ID = "my-secret-id";
    private static final HeartBeatEndpointDetails ENDPOINT_DETAILS = new HeartBeatEndpointDetails("user", "password", "endpoint", "dbname", 5432);

    @Mock
    private SecretsManagerClient secretsManagerClient;
    @Mock
    private PostgresJdbcClientProvider postgresJdbcClientProvider;
    @Mock
    private JdbcClient jdbcClient;
    @Mock
    private LambdaLogger logger;

    private PostgresTickleService underTest;

    @BeforeEach
    void setUp() {
        underTest = new PostgresTickleService(secretsManagerClient, postgresJdbcClientProvider);
    }

    @Test
    void tickleShouldGetTheSecret() {
        when(secretsManagerClient.getSecret(SECRET_ID, HeartBeatEndpointDetails.class)).thenReturn(ENDPOINT_DETAILS);
        when(postgresJdbcClientProvider.buildJdbcClient(ENDPOINT_DETAILS)).thenReturn(jdbcClient);

        underTest.tickle(logger, SECRET_ID);

        verify(secretsManagerClient, times(1)).getSecret(SECRET_ID, HeartBeatEndpointDetails.class);
    }

    @Test
    void tickleShouldTickleTheDatabase() {
        when(secretsManagerClient.getSecret(SECRET_ID, HeartBeatEndpointDetails.class)).thenReturn(ENDPOINT_DETAILS);
        when(postgresJdbcClientProvider.buildJdbcClient(ENDPOINT_DETAILS)).thenReturn(jdbcClient);

        underTest.tickle(logger, SECRET_ID);

        String expectedTickleQuery = "SELECT pg_logical_emit_message(true, 'heartbeat', 'ping')";
        verify(jdbcClient, times(1)).executeQueryAndCloseConnection(expectedTickleQuery);
    }

}