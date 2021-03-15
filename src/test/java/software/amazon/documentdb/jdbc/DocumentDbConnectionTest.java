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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbConnectionTest extends DocumentDbFlapDoodleTest {

    private static final String HOSTNAME = "localhost";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "testDb";
    private static final String COLLECTION_NAME = "COLLECTION";
    private static final DocumentDbConnectionProperties VALID_CONNECTION_PROPERTIES = new DocumentDbConnectionProperties();
    private static Connection basicConnection;

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
    }

    /**
     * Tests isValid() when connected to a local MongoDB instance.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testIsValidWhenConnectionIsValid() throws SQLException {
        final DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbDriver.DOCUMENT_DB_SCHEME, VALID_CONNECTION_PROPERTIES);
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
        final DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbDriver.DOCUMENT_DB_SCHEME, VALID_CONNECTION_PROPERTIES);
        Assertions.assertThrows(SQLException.class, () -> connection.isValid(-1));
    }

    /**
     * Tests close() when connected to a local mongoDB instance and Connection is not yet closed.
     *
     * @throws SQLException if an error occurs instantiating a Connection.
     */
    @Test
    void testClose() throws SQLException {
        final DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbDriver.DOCUMENT_DB_SCHEME, VALID_CONNECTION_PROPERTIES);
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

        final DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbDriver.DOCUMENT_DB_SCHEME, properties);
        Assertions.assertNotNull(connection);
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

        final DocumentDbConnection connection = (DocumentDbConnection) DriverManager.getConnection(
                DocumentDbDriver.DOCUMENT_DB_SCHEME, properties);
        Assertions.assertNotNull(connection);
    }

    /** Tests constructor when passed an invalid database name. */
    @Test
    void testConnectionWithInvalidDatabase() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);
        properties.setDatabase(" ");

        Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(
                DocumentDbDriver.DOCUMENT_DB_SCHEME, properties));
    }

    /** Tests constructor when passed invalid credentials. */
    @Disabled // TODO: Determine if connection can/should be tested when creating a connection.
    @Test
    void testConnectionWithInvalidCredentials() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties(VALID_CONNECTION_PROPERTIES);
        properties.setUser("invalidUser");

        Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(
                DocumentDbDriver.DOCUMENT_DB_SCHEME, properties));
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
        final DocumentDbDatabaseMetadata metadata = (DocumentDbDatabaseMetadata) basicConnection.getMetaData();
        metadata.getSQLKeywords();
        Assertions.assertNotNull(metadata);

        Assertions.assertEquals("jdbc:documentdb:", metadata.getURL());
        Assertions.assertEquals(USERNAME, metadata.getUserName());

        final ResultSet procedures = metadata.getProcedures(null, null, null);
        Assertions.assertFalse(procedures.next());
        final ResultSet catalogs = metadata.getCatalogs();
        Assertions.assertTrue(catalogs.next());
        Assertions.assertNull(catalogs.getString(1));
        Assertions.assertFalse(catalogs.next());
        final ResultSet columnPrivileges = metadata.getColumnPrivileges(null,
                null, null, null);
        Assertions.assertFalse(columnPrivileges.next());
    }

    /**
     * Tests getting primary keys from database.
     */
    @Disabled // TODO: Fix primary keys metadata.
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
        Assertions.assertNull(tables.getString(1));
        Assertions.assertEquals("metadata", tables.getString(2));
        Assertions.assertEquals("COLUMNS", tables.getString(3));
        Assertions.assertEquals("SYSTEM TABLE", tables.getString(4));
        Assertions.assertTrue(tables.next());
        Assertions.assertNull(tables.getString(1));
        Assertions.assertEquals("metadata", tables.getString(2));
        Assertions.assertEquals("TABLES", tables.getString(3));
        Assertions.assertEquals("SYSTEM TABLE", tables.getString(4));
        Assertions.assertTrue(tables.next());
        Assertions.assertNull(tables.getString(1));
        Assertions.assertEquals("testDb", tables.getString(2));
        Assertions.assertEquals("COLLECTION", tables.getString(3));
        Assertions.assertEquals("TABLE", tables.getString(4));
        Assertions.assertFalse(tables.next());
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
        Assertions.assertTrue(tableTypes.next());
        Assertions.assertEquals("VIEW", tableTypes.getString(1));
        Assertions.assertFalse(tableTypes.next());
    }
}
