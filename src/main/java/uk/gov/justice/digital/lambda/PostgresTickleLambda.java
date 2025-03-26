package uk.gov.justice.digital.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.jdbc.PostgresJdbcClientProvider;
import uk.gov.justice.digital.clients.secretsmanager.AWSSecretsManagerProvider;
import uk.gov.justice.digital.clients.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.services.postgrestickle.PostgresTickleService;

public class PostgresTickleLambda implements RequestHandler<Void, Void> {

    private final PostgresTickleService postgresTickleService;

    public PostgresTickleLambda() {
        SecretsManagerClient secretsManagerClient = new SecretsManagerClient(new AWSSecretsManagerProvider());
        this.postgresTickleService = new PostgresTickleService(secretsManagerClient, new PostgresJdbcClientProvider());
    }

    @Override
    public Void handleRequest(Void input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Starting postgres tickle lambda handler", LogLevel.DEBUG);
        String heartbeatEndpointSecretId = System.getenv("HEARTBEAT_ENDPOINT_SECRET_ID");
        postgresTickleService.tickle(logger, heartbeatEndpointSecretId);
        logger.log("Finished postgres tickle lambda handler", LogLevel.DEBUG);
        return null;
    }
}
