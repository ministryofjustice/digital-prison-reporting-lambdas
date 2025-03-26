package uk.gov.justice.digital.services.postgrestickle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class JdbcEndpointDetailsTest {
    private static final JdbcEndpointDetails ENDPOINT_DETAILS = new JdbcEndpointDetails("match", "match", "match", "match", 5432);

    @Test
    void equalsShouldBeTrueWhenAllFieldsMatch() {
        JdbcEndpointDetails underTest = new JdbcEndpointDetails("match", "match", "match", "match", 5432);
        assertEquals(ENDPOINT_DETAILS, underTest);
    }

    @ParameterizedTest
    @CsvSource({
            "match,match,match,match,0",
            "match,match,match,mismatch,5432",
            "match,match,mismatch,match,5432",
            "match,mismatch,match,match,5432",
            "mismatch,match,match,match,5432",
    })
    void equalsShouldBeFalseForMismatch(String username, String password, String endpoint, String dbname, int port) {
        JdbcEndpointDetails underTest = new JdbcEndpointDetails(username, password, endpoint, dbname, port);
        assertNotEquals(ENDPOINT_DETAILS, underTest);
    }

    @Test
    void hashCodeShouldBeEqualWhenAllFieldsMatch() {
        JdbcEndpointDetails underTest = new JdbcEndpointDetails("match", "match", "match", "match", 5432);
        assertEquals(ENDPOINT_DETAILS.hashCode(), underTest.hashCode());
    }

    @ParameterizedTest
    @CsvSource({
            "match,match,match,match,0",
            "match,match,match,mismatch,5432",
            "match,match,mismatch,match,5432",
            "match,mismatch,match,match,5432",
            "mismatch,match,match,match,5432",
    })
    void hashCodeShouldBeDifferentForMismatch(String username, String password, String endpoint, String dbname, int port) {
        JdbcEndpointDetails underTest = new JdbcEndpointDetails(username, password, endpoint, dbname, port);
        assertNotEquals(ENDPOINT_DETAILS.hashCode(), underTest.hashCode());
    }
}