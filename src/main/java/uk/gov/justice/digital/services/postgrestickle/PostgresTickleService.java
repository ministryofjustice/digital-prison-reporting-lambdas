package uk.gov.justice.digital.services.postgrestickle;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.jdbc.JdbcClient;
import uk.gov.justice.digital.clients.jdbc.PostgresJdbcClientProvider;
import uk.gov.justice.digital.clients.secretsmanager.SecretsManagerClient;

public class PostgresTickleService {

    private static final String HEARTBEAT_QUERY = "SELECT pg_logical_emit_message(true, 'heartbeat', 'ping')";

    private final SecretsManagerClient secretsManagerClient;
    private final PostgresJdbcClientProvider postgresJdbcClientProvider;

    public PostgresTickleService(SecretsManagerClient secretsManagerClient, PostgresJdbcClientProvider postgresJdbcClientProvider) {
        this.secretsManagerClient = secretsManagerClient;
        this.postgresJdbcClientProvider = postgresJdbcClientProvider;
    }

    public void tickle(LambdaLogger logger, String secretId) {
        logger.log("Retrieving endpoint details secret " + secretId, LogLevel.DEBUG);
        HeartBeatEndpointDetails endpointDetails = secretsManagerClient.getSecret(secretId, HeartBeatEndpointDetails.class);
        JdbcClient jdbcClient = postgresJdbcClientProvider.buildJdbcClient(endpointDetails);
        logger.log("Executing heartbeat query " + HEARTBEAT_QUERY, LogLevel.DEBUG);
        jdbcClient.executeQueryAndCloseConnection(HEARTBEAT_QUERY);
    }
}
