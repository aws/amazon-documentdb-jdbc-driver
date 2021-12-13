/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironmentFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaReader;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbConnectionTest extends DocumentDbFlapDoodleTest {

    private static final String HOSTNAME = "localhost";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "testDb";
    private static final String COLLECTION_NAME = "COLLECTION";
    private static final DocumentDbConnectionProperties VALID_CONNECTION_PROPERTIES = new DocumentDbConnectionProperties();
    private static Connection basicConnection;
    private static final String DOC_DB_USER_PROPERTY = "DOC_DB_USER";
    private static final String DOC_DB_HOST_PROPERTY = "DOC_DB_HOST";
    private static final String DOC_DB_PRIV_KEY_FILE_PROPERTY = "DOC_DB_PRIV_KEY_FILE";

    /** Initializes the test class. */
    @BeforeAll
    public static void initialize() throws SQLException {

        VALID_CONNECTION_PROPERTIES.setUser(USERNAME);
        VALID_CONNECTION_PROPERTIES.setPassword(PASSWORD);
        VALID_CONNECTION_PROPERTIES.setDatabase(DATABASE);
        VALID_CONNECTION_PROPERTIES.setTlsEnabled("false");
        VALID_CONNECTION_PROPERTIES.setHostname(HOSTNAME + ":" + getMongoPort());

        // Add 1 valid user so we can successfully authenticate.
        createUser(DATABASE, USERNAME, PASSWORD);
        prepareSimpleConsistentData(DATABASE, COLLECTION_NAME,
                5, USERNAME, PASSWORD);

        final String connectionString = String.format(
                "jdbc:documentdb://%s:%s@%s:%s/%s?tls=false", USERNAME, PASSWORD, HOSTNAME, getMongoPort(), DATABASE);
        basicConnection = DriverManager.getConnection(connectionString);
        // Ensure we have the initial schema.
        Assertions.assertNotNull(basicConnection.getMetaData());
    }

    @AfterAll
    static void afterAll() throws Exception {
        try (DocumentDbSchemaWriter schemaWriter = new DocumentDbSchemaWriter(
                VALID_CONNECTION_PROPERTIES, null)) {
            schemaWriter.remove(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
        }
        basicConnection.close();
    }

    /**
     * Tests isValid() when connected to a local MongoDB instance.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testIsValidWhenConnectionIsValid() throws SQLException {
        try (DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, VALID_CONNECTION_PROPERTIES)) {
            // NOTE: Observed approximate 10 … 11 seconds delay before first heartbeat is returned.
            final int timeoutSeconds = 15;
            Assertions.assertTrue(connection.isValid(timeoutSeconds));
        }
    }

    /**
     * Tests isValid() when connected to a local MongoDB instance but timeout is negative.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testIsValidWhenTimeoutIsNegative() throws SQLException {
        try (DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, VALID_CONNECTION_PROPERTIES)) {
            Assertions.assertThrows(SQLException.class, () -> connection.isValid(-1));
        }
    }

    /**
     * Tests close() when connected to a local mongoDB instance and Connection is not yet closed.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testClose() throws SQLException {
        final DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, VALID_CONNECTION_PROPERTIES);
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

        try (DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, properties)) {
            Assertions.assertNotNull(connection);
        }
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

        try (DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, properties)) {
            Assertions.assertNotNull(connection);
        }
    }

    /** Tests constructor when passed an invalid database name. */
    @Test
    void testConnectionWithInvalidDatabase() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);
        properties.setDatabase(" ");

        Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, properties));
    }

    /** Tests constructor when passed invalid credentials. */
    @Test
    void testConnectionWithInvalidCredentials() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);
        properties.setUser("invalidUser");

        Assertions.assertTrue(
                Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(
                        DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, properties))
                        .getMessage()
                        .contains("Authorization failed for user"));
    }

    /**
     * Test for connection.getSchema() and getCatalog
     */
    @Test
    @DisplayName("Tests that catalog is null, and schema is equal to the database name.")
    void testGetMetadataSchema() throws SQLException {

        final String catalog = basicConnection.getCatalog();
        Assertions.assertNull(catalog);

        final String schema = basicConnection.getSchema();
        Assertions.assertEquals(DATABASE, schema);
    }

    /**
     * Test for connection.getMetadata() for basic properties.
     */
    @Test
    @DisplayName("Tests simple metadata of a database connection.")
    void testGetMetadata() throws SQLException {
        final DocumentDbDatabaseMetaData metadata = (DocumentDbDatabaseMetaData) basicConnection.getMetaData();
        Assertions.assertEquals("", metadata.getSQLKeywords());
        Assertions.assertNotNull(metadata);

        final String connectionString = String.format(
                "jdbc:documentdb://%s@%s:%s/%s?tls=false", USERNAME, HOSTNAME, getMongoPort(), DATABASE);

        Assertions.assertEquals(connectionString, metadata.getURL());
        Assertions.assertEquals(USERNAME, metadata.getUserName());

        final ResultSet procedures = metadata.getProcedures(null, null, null);
        Assertions.assertFalse(procedures.next());
        final ResultSet catalogs = metadata.getCatalogs();
        // No records indicate we don't support/use catalogs.
        Assertions.assertFalse(catalogs.next());
        final ResultSet columnPrivileges = metadata.getColumnPrivileges(null,
                null, null, null);
        Assertions.assertFalse(columnPrivileges.next());
    }

    /**
     * Tests getting primary keys from database.
     */
    @Test
    @DisplayName("Tests that metadata can return primary keys.")
    void testGetPrimaryKeys() throws SQLException {
        final ResultSet primaryKeys = basicConnection.getMetaData()
                .getPrimaryKeys(null, null, COLLECTION_NAME);
        Assertions.assertTrue(primaryKeys.next());
        Assertions.assertEquals("COLLECTION__id", primaryKeys.getString(4));
        Assertions.assertEquals(1, primaryKeys.getShort(5));
    }

    /**
     * Tests metadata for tables of database.
     */
    @Test
    @DisplayName("Tests the database metadata contains the expected tables.")
    void testGetMetadataTables() throws SQLException {
        final ResultSet tables = basicConnection.getMetaData().getTables(null, null, null, null);
        Assertions.assertTrue(tables.next());
        // Test by column index
        Assertions.assertNull(tables.getString(1));
        Assertions.assertEquals("testDb", tables.getString(2));
        Assertions.assertEquals("COLLECTION", tables.getString(3));
        Assertions.assertEquals("TABLE", tables.getString(4));
        // Test by column label, case-insensitive
        Assertions.assertNull(tables.getString("TABLE_cat"));
        Assertions.assertEquals("testDb", tables.getString("TABLE_SCHEM"));
        Assertions.assertEquals("COLLECTION", tables.getString("table_name"));
        Assertions.assertEquals("TABLE", tables.getString("table_TYPE"));
        Assertions.assertFalse(tables.next());
    }

    @Test
    @DisplayName("Tests that a statement can be created given valid type and concurrency")
    void testStatement() throws SQLException {
        try (Statement statement = basicConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            Assertions.assertNotNull(statement);
        }
        try (Statement ignored = basicConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            Assertions.fail("Exception should be thrown.");
        } catch (Exception e) {
            Assertions.assertEquals(SqlError.lookup(SqlError.UNSUPPORTED_RESULT_SET_TYPE),e.getMessage());
        }
        try (Statement ignored = basicConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
           Assertions.fail("Exception should be thrown.");
        } catch (Exception e) {
            Assertions.assertEquals(SqlError.lookup(SqlError.UNSUPPORTED_RESULT_SET_TYPE),e.getMessage());
        }

    }

    @Test
    @DisplayName("Tests that a prepared statement can be created given valid type and concurrency")
    void testPreparedStatement() throws SQLException {
        final String sql = "SELECT * FROM " + COLLECTION_NAME;
        try (PreparedStatement statement = basicConnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            Assertions.assertNotNull(statement);
        }
        try (PreparedStatement ignored = basicConnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            Assertions.fail("Exception should be thrown.");
        } catch (Exception e) {
            Assertions.assertEquals(SqlError.lookup(SqlError.UNSUPPORTED_RESULT_SET_TYPE), e.getMessage());
        }
        try (PreparedStatement ignored = basicConnection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            Assertions.fail("Exception should be thrown.");
        } catch (Exception e) {
            Assertions.assertEquals(SqlError.lookup(SqlError.UNSUPPORTED_RESULT_SET_TYPE), e.getMessage());
        }
    }

    /**
     * Tests metadata for table types.
     */
    @Test
    @DisplayName("Tests that the table types table contains table and view.")
    void testMetadataGetTableTypes() throws SQLException {
        final ResultSet tableTypes = basicConnection.getMetaData().getTableTypes();
        Assertions.assertEquals("TABLE_TYPE", tableTypes.getMetaData().getColumnName(1));
        Assertions.assertTrue(tableTypes.next());
        Assertions.assertEquals("TABLE", tableTypes.getString(1));
        Assertions.assertFalse(tableTypes.next());
    }

    @ParameterizedTest(name = "testSshTunnelOptions - [{index}] - {arguments}")
    @DisplayName("Tests SSH tunnel options")
    @MethodSource("getDocumentDb40SshTunnelEnvironmentSourceOrNull")
    void testSshTunnelOptions(final DocumentDbTestEnvironment environment) throws SQLException {
        // NOTE: a "null" environment means it isn't configured to run. So bypass.
        if (environment == null) {
            return;
        }
        final DocumentDbConnectionProperties properties = getInternalSSHTunnelProperties(environment);
        try (Connection connection = DriverManager.getConnection("jdbc:documentdb:", properties)) {
            Assertions.assertTrue(connection instanceof DocumentDbConnection);
            Assertions.assertTrue(connection.isValid(10));
        }
    }

    @ParameterizedTest(name = "testSshTunnelInvalidOptions - [{index}] - {arguments}")
    @DisplayName("Tests SSH tunnel with invalid options")
    @MethodSource("getDocumentDb40SshTunnelEnvironmentSourceOrNull")
    void testSshTunnelInvalidOptions(final DocumentDbTestEnvironment environment) throws SQLException {
        // NOTE: a "null" environment means it isn't configured to run. So bypass.
        if (environment == null) {
            return;
        }
        final DocumentDbConnectionProperties properties = getInternalSSHTunnelProperties(environment);
        // Test incorrect private key file path.
        properties.setSshPrivateKeyFile("~/certs/other.pem");
        Assertions.assertEquals(SqlError.lookup(SqlError.SSH_PRIVATE_KEY_FILE_NOT_FOUND, "~/certs/other.pem"),
                Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:documentdb:", properties)).getMessage());

        // Test incorrect known hosts path.
        properties.setSshPrivateKeyFile(System.getenv(DOC_DB_PRIV_KEY_FILE_PROPERTY));
        properties.setSshStrictHostKeyChecking("true");
        properties.setSshKnownHostsFile("~/.ssh/unknown_hosts");
        Assertions.assertEquals(SqlError.lookup(SqlError.KNOWN_HOSTS_FILE_NOT_FOUND, "~/.ssh/unknown_hosts"),
                Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:documentdb:", properties)).getMessage());

        // Test where TLSAllowInvalidHostnames is disabled - host will not match certificate.
        properties.setSshStrictHostKeyChecking("false");
        properties.setTlsAllowInvalidHostnames("false");
        Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:documentdb:", properties));
    }

    @Test()
    @DisplayName("Tests refreshing the schema from a connection.")
    void testRefreshSchema() throws Exception {
        final DocumentDbConnectionProperties properties =
                new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);

        int expectedVersion = 1;
        try (DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(
                VALID_CONNECTION_PROPERTIES, null)) {
            final DocumentDbSchema schema = schemaReader.read(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
            Assertions.assertNotNull(schema);
            Assertions.assertEquals(expectedVersion, schema.getSchemaVersion());
        }

        properties.setRefreshSchema("true");
        try (DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, properties)) {
            final int timeoutSeconds = 15;
            Assertions.assertTrue(connection.isValid(timeoutSeconds));
            Assertions.assertNotNull(connection.getDatabaseMetadata());
        }
        expectedVersion++;

        try (DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(
                VALID_CONNECTION_PROPERTIES, null)) {
            final DocumentDbSchema schema = schemaReader.read(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
            Assertions.assertNotNull(schema);
            Assertions.assertEquals(expectedVersion, schema.getSchemaVersion());
        }

        properties.setRefreshSchema("true");
        try (DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, properties)) {
            final int timeoutSeconds = 15;
            Assertions.assertTrue(connection.isValid(timeoutSeconds));
            Assertions.assertNotNull(connection.getDatabaseMetadata());
        }
        expectedVersion++;

        try (DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(
                VALID_CONNECTION_PROPERTIES, null)) {
            final DocumentDbSchema schema = schemaReader.read(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
            Assertions.assertNotNull(schema);
            Assertions.assertEquals(expectedVersion, schema.getSchemaVersion());
        }

        properties.setRefreshSchema("true");
        try (DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, properties)) {
            final int timeoutSeconds = 15;
            Assertions.assertTrue(connection.isValid(timeoutSeconds));
            Assertions.assertNotNull(connection.getDatabaseMetadata());
        }
        expectedVersion++;

        try (DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(
                VALID_CONNECTION_PROPERTIES, null)) {
            final DocumentDbSchema schema = schemaReader.read(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
            Assertions.assertNotNull(schema);
            Assertions.assertEquals(expectedVersion, schema.getSchemaVersion());
        }

        properties.setRefreshSchema("false");
        try (DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME, properties)) {
            final int timeoutSeconds = 15;
            Assertions.assertTrue(connection.isValid(timeoutSeconds));
            Assertions.assertNotNull(connection.getDatabaseMetadata());
        }

        try (DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(
                VALID_CONNECTION_PROPERTIES, null)) {
            final DocumentDbSchema schema = schemaReader.read(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
            Assertions.assertNotNull(schema);
            Assertions.assertEquals(expectedVersion, schema.getSchemaVersion());
        }

        // Always false unless value is non-null and case-insensitive equal to "true".
        properties.setRefreshSchema("notTrue");
        Assertions.assertFalse(properties.getRefreshSchema());
    }

    private Stream<DocumentDbTestEnvironment> getDocumentDb40SshTunnelEnvironmentSourceOrNull() {
        if (DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream()
                .anyMatch(e -> e ==  DocumentDbTestEnvironmentFactory
                        .getDocumentDb40SshTunnelEnvironment())) {
            return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream()
                    .filter(e -> e == DocumentDbTestEnvironmentFactory
                            .getDocumentDb40SshTunnelEnvironment());
        } else {
            return Stream.of((DocumentDbTestEnvironment) null);
        }
    }

    private static DocumentDbConnectionProperties getInternalSSHTunnelProperties(
            final DocumentDbTestEnvironment environment)
            throws SQLException {
        final String docDbRemoteHost = System.getenv(DOC_DB_HOST_PROPERTY);
        final String docDbSshUserAndHost = System.getenv(DOC_DB_USER_PROPERTY);
        final String docDbPrivKeyFile = System.getenv(DOC_DB_PRIV_KEY_FILE_PROPERTY);
        final int userSeparatorIndex = docDbSshUserAndHost.indexOf('@');
        final String sshUser = docDbSshUserAndHost.substring(0, userSeparatorIndex);
        final String sshHostname = docDbSshUserAndHost.substring(userSeparatorIndex + 1);
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(environment.getJdbcConnectionString());
        properties.setHostname(docDbRemoteHost);
        properties.setSshUser(sshUser);
        properties.setSshHostname(sshHostname);
        properties.setSshPrivateKeyFile(docDbPrivKeyFile);
        properties.setSshStrictHostKeyChecking("false");
        return properties;
    }
}
