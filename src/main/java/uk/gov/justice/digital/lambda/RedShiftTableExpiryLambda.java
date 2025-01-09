package uk.gov.justice.digital.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import uk.gov.justice.digital.clients.redshift.ExternalTableQueryExecutor;
import uk.gov.justice.digital.clients.s3.S3Client;
import uk.gov.justice.digital.services.RedShiftTableExpiryService;

import java.util.Map;

/**
 * Lambda function to remove RedShift external tables that are past their expiry time.
 */
@SuppressWarnings("unused")
public class RedShiftTableExpiryLambda implements RequestHandler<Map<String, Object>, Void> {
    private static final String CLUSTER_ID_VAR_NAME = "CLUSTER_ID";
    private static final String DB_NAME_VAR_NAME = "DB_NAME";
    private static final String CREDENTIAL_SECRET_ARN_VAR_NAME = "CREDENTIAL_SECRET_ARN";
    private static final String EXPIRY_SECONDS_VAR_NAME = "EXPIRY_SECONDS";

    private final RedShiftTableExpiryService service;

    @SuppressWarnings("unused")
    public RedShiftTableExpiryLambda() {
        var clusterId = System.getenv(CLUSTER_ID_VAR_NAME);
        var databaseName = System.getenv(DB_NAME_VAR_NAME);
        var secretArn = System.getenv(CREDENTIAL_SECRET_ARN_VAR_NAME);
        var expirySeconds =  Integer.parseInt(System.getenv(EXPIRY_SECONDS_VAR_NAME));

        var dataClient = RedshiftDataClient.builder()
                .region(Region.EU_WEST_2)
                .build();
        var queryExecutor = new ExternalTableQueryExecutor(
                dataClient,
                clusterId,
                databaseName,
                secretArn
        );

        this.service = new RedShiftTableExpiryService(
                new S3Client(),
                queryExecutor,
                expirySeconds
        );
    }

    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {

        LambdaLogger logger = context.getLogger();
        logger.log("Started expired table removal", LogLevel.INFO);

        this.service.removeExpiredExternalTables(logger);

        logger.log("Finished expired table removal", LogLevel.INFO);
        return null;
    }
}
