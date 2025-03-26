package uk.gov.justice.digital.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import uk.gov.justice.digital.clients.jdbc.PostgresJdbcClientProvider;
import uk.gov.justice.digital.clients.secretsmanager.AWSSecretsManagerProvider;
import uk.gov.justice.digital.clients.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.services.postgrestickle.PostgresTickleService;

import java.util.Map;

import static uk.gov.justice.digital.common.Utils.SECRET_ID_KEY;
import static uk.gov.justice.digital.common.Utils.getOptionalString;
import static uk.gov.justice.digital.common.Utils.getOrThrow;

public class PostgresTickleLambda implements RequestHandler<Map<String, Object>, Void> {

    private final PostgresTickleService postgresTickleService;

    public PostgresTickleLambda() {
        SecretsManagerClient secretsManagerClient = new SecretsManagerClient(new AWSSecretsManagerProvider());
        this.postgresTickleService = new PostgresTickleService(secretsManagerClient, new PostgresJdbcClientProvider());
    }

    @Override
    public Void handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Starting postgres tickle lambda handler", LogLevel.DEBUG);
        String heartbeatEndpointSecretId = getOrThrow(input, SECRET_ID_KEY, String.class);
        postgresTickleService.tickle(logger, heartbeatEndpointSecretId);
        logger.log("Finished postgres tickle lambda handler", LogLevel.DEBUG);
        return null;
    }
}
