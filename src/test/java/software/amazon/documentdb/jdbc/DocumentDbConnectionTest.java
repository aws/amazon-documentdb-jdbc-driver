package software.amazon.documentdb.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class DocumentDbConnectionTest extends DocumentDbTest {

    private static final String HOSTNAME = "localhost";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "database";
    private static final DocumentDbConnectionProperties VALID_CONNECTION_PROPERTIES = new DocumentDbConnectionProperties();

    /** Initializes the test class. */
    @BeforeAll
    public static void initialize() throws IOException {
        VALID_CONNECTION_PROPERTIES.setUser(USERNAME);
        VALID_CONNECTION_PROPERTIES.setPassword(PASSWORD);
        VALID_CONNECTION_PROPERTIES.setDatabase(DATABASE);
        VALID_CONNECTION_PROPERTIES.setTlsEnabled("false");

        // Start mongod instance and get the port number for the connection.
        startMongoDbInstance(true);
        VALID_CONNECTION_PROPERTIES.setHostname(HOSTNAME + ":" + getMongoPort());

        // Add 1 valid user so we can successfully authenticate.
        createUser(DATABASE, USERNAME, PASSWORD);
    }

    /**
     * Clean-up
     */
    @AfterAll
    public static void cleanup() {
        stopMongoDbInstance();
    }

    /**
     * Tests isValid() when connected to a local MongoDB instance.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testIsValidWhenConnectionIsValid() throws SQLException {
        final DocumentDbConnection connection = new DocumentDbConnection(VALID_CONNECTION_PROPERTIES);
        // NOTE: Observed approximate 10 .. 11 seconds delay before first heartbeat is returned.
        final int timeoutSeconds = 15;
        Assertions.assertTrue(connection.isValid(timeoutSeconds));
    }

    /**
     * Tests isValid() when connected to a local MongoDB instance but timeout is negative.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testIsValidWhenTimeoutIsNegative() throws SQLException {
        final DocumentDbConnection connection = new DocumentDbConnection((VALID_CONNECTION_PROPERTIES));
        Assertions.assertThrows(SQLException.class, () -> connection.isValid(-1));
    }

    /**
     * Tests close() when connected to a local mongoDB instance and Connection is not yet closed.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testClose() throws SQLException {
        final DocumentDbConnection connection = new DocumentDbConnection(VALID_CONNECTION_PROPERTIES);
        Assertions.assertFalse(connection.isClosed());
        connection.close();
        Assertions.assertTrue(connection.isClosed());
    }

    /**
     * Tests constructor when passed valid options.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testConnectionWithValidOptions() throws SQLException {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);
        properties.setApplicationName("test");
        properties.setLoginTimeout("10");
        properties.setRetryReadsEnabled("false");
        properties.setReadPreference(DocumentDbReadPreference.PRIMARY.getName());

        final DocumentDbConnection connection = new DocumentDbConnection(properties);
        Assertions.assertNotNull(connection);
        Assertions.assertEquals(DATABASE, connection.getMongoDatabase().getName());
    }

    /**
     * Tests constructor when passed invalid options. Invalid options are ignored.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testConnectionWithInvalidOptions() throws SQLException {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);
        properties.setReadPreference("invalidReadPreference");
        properties.setTlsEnabled("invalidBoolean");

        final DocumentDbConnection connection = new DocumentDbConnection(properties);
        Assertions.assertNotNull(connection);
        Assertions.assertEquals(DATABASE, connection.getMongoDatabase().getName());
    }

    /** Tests constructor when passed an invalid database name. */
    @Test
    void testConnectionWithInvalidDatabase() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);
        properties.setDatabase(" ");

        Assertions.assertThrows(SQLException.class, () -> new DocumentDbConnection(properties));
    }

    /** Tests constructor when passed invalid credentials. */
    @Test
    void testConnectionWithInvalidCredentials() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);
        properties.setUser("invalidUser");

        Assertions.assertThrows(SQLException.class, () -> new DocumentDbConnection(properties));
    }
}
