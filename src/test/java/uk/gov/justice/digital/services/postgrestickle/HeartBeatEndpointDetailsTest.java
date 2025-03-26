package uk.gov.justice.digital.services.postgrestickle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class HeartBeatEndpointDetailsTest {
    private static final HeartBeatEndpointDetails ENDPOINT_DETAILS = new HeartBeatEndpointDetails("match", "match", "match", "match", 5432);

    @Test
    void equalsShouldBeTrueWhenAllFieldsMatch() {
        HeartBeatEndpointDetails underTest = new HeartBeatEndpointDetails("match", "match", "match", "match", 5432);
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
        HeartBeatEndpointDetails underTest = new HeartBeatEndpointDetails(username, password, endpoint, dbname, port);
        assertNotEquals(ENDPOINT_DETAILS, underTest);
    }

    @Test
    void hashCodeShouldBeEqualWhenAllFieldsMatch() {
        HeartBeatEndpointDetails underTest = new HeartBeatEndpointDetails("match", "match", "match", "match", 5432);
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
        HeartBeatEndpointDetails underTest = new HeartBeatEndpointDetails(username, password, endpoint, dbname, port);
        assertNotEquals(ENDPOINT_DETAILS.hashCode(), underTest.hashCode());
    }

    @Test
    void gettersAndSettersShouldWork() {
        HeartBeatEndpointDetails underTest = new HeartBeatEndpointDetails("user", "pass", "endpoint", "db", 5432);
        assertEquals("user", underTest.getUsername());
        assertEquals("pass", underTest.getPassword());
        assertEquals("endpoint", underTest.getHeartBeatEndpoint());
        assertEquals("db", underTest.getDbName());
        assertEquals(5432, underTest.getPort());
    }


}