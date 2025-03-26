package uk.gov.justice.digital.services.postgrestickle;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HeartBeatEndpointDetails {
    private String username;
    private String password;
    @JsonProperty("heartbeat_endpoint")
    private String heartBeatEndpoint;
    @JsonProperty("db_name")
    private String dbName;
    private int port;

    public HeartBeatEndpointDetails(String username, String password, String heartBeatEndpoint, String dbName, int port) {
        this.username = username;
        this.password = password;
        this.heartBeatEndpoint = heartBeatEndpoint;
        this.dbName = dbName;
        this.port = port;
    }

    public HeartBeatEndpointDetails() {
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHeartBeatEndpoint() {
        return heartBeatEndpoint;
    }

    public void setHeartBeatEndpoint(String heartBeatEndpoint) {
        this.heartBeatEndpoint = heartBeatEndpoint;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        HeartBeatEndpointDetails that = (HeartBeatEndpointDetails) o;
        return port == that.port && Objects.equals(username, that.username) && Objects.equals(password, that.password) && Objects.equals(heartBeatEndpoint, that.heartBeatEndpoint) && Objects.equals(dbName, that.dbName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, password, heartBeatEndpoint, dbName, port);
    }
}
