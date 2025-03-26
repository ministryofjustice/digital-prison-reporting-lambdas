package uk.gov.justice.digital.clients.jdbc;

import uk.gov.justice.digital.services.postgrestickle.HeartBeatEndpointDetails;

import static java.lang.String.format;

public class PostgresJdbcClientProvider {

    public JdbcClient buildJdbcClient(HeartBeatEndpointDetails endpointDetails) {
        return new JdbcClient(postgresJdbcUrl(endpointDetails), endpointDetails.getUsername(), endpointDetails.getPassword());
    }

    // Visible for testing
    static String postgresJdbcUrl(HeartBeatEndpointDetails endpointDetails) {
        return format("jdbc:postgresql://%s:%d/%s", endpointDetails.getHeartBeatEndpoint(), endpointDetails.getPort(), endpointDetails.getDbName());
    }
}
