package uk.gov.justice.digital.clients.jdbc;

import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.services.postgrestickle.HeartBeatEndpointDetails;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgresJdbcClientProviderTest {

    @Test
    void postgresJdbcUrl() {
        HeartBeatEndpointDetails endpointDetails = new HeartBeatEndpointDetails("user", "password", "endpoint", "dbname", 5432);
        String url = PostgresJdbcClientProvider.postgresJdbcUrl(endpointDetails);

        assertEquals("jdbc:postgresql://endpoint:5432/dbname", url);
    }

}