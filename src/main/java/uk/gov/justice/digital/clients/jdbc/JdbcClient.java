package uk.gov.justice.digital.clients.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcClient {

    private final String url;
    private final String user;
    private final String password;

    public JdbcClient(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     * Executes a query, disregarding any returned ResultSet, and closes the connection that was opened just for this query.
     * @param query The query to execute
     */
    public void executeQueryAndCloseConnection(String query) {
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            try(Statement statement = connection.createStatement()) {
                statement.execute(query);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
