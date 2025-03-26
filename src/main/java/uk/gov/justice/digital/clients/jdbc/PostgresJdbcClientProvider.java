package uk.gov.justice.digital.clients.jdbc;

import uk.gov.justice.digital.services.postgrestickle.JdbcEndpointDetails;

import static java.lang.String.format;

public class PostgresJdbcClientProvider {

    public JdbcClient buildJdbcClient(JdbcEndpointDetails endpointDetails) {
        return new JdbcClient(postgresJdbcUrl(endpointDetails), endpointDetails.getUsername(), endpointDetails.getPassword());
    }

    // Visible for testing
    static String postgresJdbcUrl(JdbcEndpointDetails endpointDetails) {
        return format("jdbc:postgresql://%s:%d/%s", endpointDetails.getHeartBeatEndpoint(), endpointDetails.getPort(), endpointDetails.getDbName());
    }
}
