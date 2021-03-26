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
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbDatabaseMetaDataTest extends DocumentDbFlapDoodleTest {

    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "testDb";
    private static final String COLLECTION_NAME = "COLLECTION";
    private static final String HOSTNAME = "localhost";

    private static Connection connection;
    private static DatabaseMetaData metadata;

    /** Initializes the test class. */
    @BeforeAll
    public static void initialize() throws SQLException {
        createUser(DATABASE, USERNAME, PASSWORD);
        prepareSimpleConsistentData(DATABASE, COLLECTION_NAME,
                5, USERNAME, PASSWORD);
        final String connectionString = String.format(
                "jdbc:documentdb://%s:%s@%s:%s/%s?tls=false", USERNAME, PASSWORD, HOSTNAME, getMongoPort(), DATABASE);
        connection = DriverManager.getConnection(connectionString);
        metadata = connection.getMetaData();
    }

    @AfterAll
    static void afterAll() throws SQLException {
        connection.close();
    }

    /**
     * Tests for basic metadata fields.
     */
    @Test
    @DisplayName("Tests basic common properties of a database.")
    void testBasicMetadata() throws SQLException {
        Assertions.assertEquals("DocumentDB", metadata.getDatabaseProductName());
        Assertions.assertEquals("4.0", metadata.getDatabaseProductVersion());
        Assertions.assertEquals("DocumentDB JDBC Driver", metadata.getDriverName());
        Assertions.assertNotNull(metadata.getSQLKeywords());
        Assertions.assertNotNull(metadata.getNumericFunctions());
        Assertions.assertNotNull(metadata.getStringFunctions());
        Assertions.assertNotNull(metadata.getTimeDateFunctions());
        Assertions.assertEquals("\\", metadata.getSearchStringEscape());
        Assertions.assertEquals("",metadata.getExtraNameCharacters());
        Assertions.assertEquals("catalog", metadata.getCatalogTerm());
        Assertions.assertEquals(".", metadata.getCatalogSeparator());
        Assertions.assertEquals(0, metadata.getMaxRowSize());
        Assertions.assertEquals(4, metadata.getDatabaseMajorVersion());
        Assertions.assertEquals(0, metadata.getDatabaseMinorVersion());
        Assertions.assertEquals(4, metadata.getJDBCMajorVersion());
        Assertions.assertEquals(2, metadata.getJDBCMinorVersion());
    }

    /**
     * Tests columns of getProcedures().
     */
    @Test
    @DisplayName("Tests the correct columns of getProcedures.")
    void testGetProcedures() throws SQLException {
        final ResultSet procedures = metadata.getProcedures(null, null, null);
        final ResultSetMetaData proceduresMetadata = procedures.getMetaData();
        Assertions.assertEquals("PROCEDURE_CAT", proceduresMetadata.getColumnName(1));
        Assertions.assertEquals("PROCEDURE_SCHEM", proceduresMetadata.getColumnName(2));
        Assertions.assertEquals("PROCEDURE_NAME", proceduresMetadata.getColumnName(3));
        Assertions.assertEquals("REMARKS", proceduresMetadata.getColumnName(7));
        Assertions.assertEquals("PROCEDURE_TYPE", proceduresMetadata.getColumnName(8));
        Assertions.assertEquals("SPECIFIC_NAME", proceduresMetadata.getColumnName(9));
    }

    /**
     * Tests columns of getTables
     */
    @Test
    @DisplayName("Tests the correct columns of getTables.")
    void testGetTables() throws SQLException {
        final ResultSet tables = metadata.getTables(null, null, null, null);
        final ResultSetMetaData tablesMetadata = tables.getMetaData();
        Assertions.assertEquals("TABLE_CAT", tablesMetadata.getColumnName(1));
        Assertions.assertEquals("TABLE_SCHEM", tablesMetadata.getColumnName(2));
        Assertions.assertEquals("TABLE_NAME", tablesMetadata.getColumnName(3));
        Assertions.assertEquals("TABLE_TYPE", tablesMetadata.getColumnName(4));
        Assertions.assertEquals("REMARKS", tablesMetadata.getColumnName(5));
        Assertions.assertEquals("TYPE_CAT", tablesMetadata.getColumnName(6));
        Assertions.assertEquals("TYPE_SCHEM", tablesMetadata.getColumnName(7));
        Assertions.assertEquals("TYPE_NAME", tablesMetadata.getColumnName(8));
        Assertions.assertEquals("SELF_REFERENCING_COL_NAME", tablesMetadata.getColumnName(9));
        Assertions.assertEquals("REF_GENERATION", tablesMetadata.getColumnName(10));
    }

    /**
     * Tests columns of getSchemas.
     */
    @Test
    @DisplayName("Tests the correct columns of getSchemas.")
    void testGetSchemas() throws SQLException {
        final ResultSet schemas = metadata.getSchemas();
        final ResultSetMetaData schemasMetadata = schemas.getMetaData();
        Assertions.assertEquals("TABLE_SCHEM", schemasMetadata.getColumnName(1));
        Assertions.assertEquals("TABLE_CATALOG", schemasMetadata.getColumnName(2));
    }

    /**
     * Tests columns of getCatalogs()
     */
    @Test
    @DisplayName("Tests the correct columns of getCatalogs.")
    void testGetCatalogs() throws SQLException {
        final ResultSet catalogs = metadata.getCatalogs();
        final ResultSetMetaData catalogMetadata = catalogs.getMetaData();
        Assertions.assertEquals("TABLE_CAT", catalogMetadata.getColumnName(1));
    }

    /**
     * Tests columns of getColumns result set.
     */
    @Test
    @DisplayName("Tests the correct columns of getColumns.")
    void testGetColumns() throws SQLException {
        final ResultSet columns = metadata.getColumns(null, null, null, null);
        final ResultSetMetaData columnsMetadata = columns.getMetaData();

        Assertions.assertEquals("TABLE_CAT", columnsMetadata.getColumnName(1));
        Assertions.assertEquals("TABLE_SCHEM", columnsMetadata.getColumnName(2));
        Assertions.assertEquals("TABLE_NAME", columnsMetadata.getColumnName(3));
        Assertions.assertEquals("COLUMN_NAME", columnsMetadata.getColumnName(4));
        Assertions.assertEquals("DATA_TYPE", columnsMetadata.getColumnName(5));
        Assertions.assertEquals("TYPE_NAME", columnsMetadata.getColumnName(6));
        Assertions.assertEquals("COLUMN_SIZE", columnsMetadata.getColumnName(7));
        Assertions.assertEquals("BUFFER_LENGTH", columnsMetadata.getColumnName(8));
        Assertions.assertEquals("DECIMAL_DIGITS", columnsMetadata.getColumnName(9));
        Assertions.assertEquals("NUM_PREC_RADIX", columnsMetadata.getColumnName(10));
        Assertions.assertEquals("NULLABLE", columnsMetadata.getColumnName(11));
        Assertions.assertEquals("REMARKS", columnsMetadata.getColumnName(12));
        Assertions.assertEquals("COLUMN_DEF", columnsMetadata.getColumnName(13));
        Assertions.assertEquals("SQL_DATA_TYPE", columnsMetadata.getColumnName(14));
        Assertions.assertEquals("SQL_DATETIME_SUB", columnsMetadata.getColumnName(15));
        Assertions.assertEquals("CHAR_OCTET_LENGTH", columnsMetadata.getColumnName(16));
        Assertions.assertEquals("ORDINAL_POSITION", columnsMetadata.getColumnName(17));
        Assertions.assertEquals("IS_NULLABLE", columnsMetadata.getColumnName(18));
        Assertions.assertEquals("SCOPE_CATALOG", columnsMetadata.getColumnName(19));
        Assertions.assertEquals("SCOPE_SCHEMA", columnsMetadata.getColumnName(20));
        Assertions.assertEquals("SCOPE_TABLE", columnsMetadata.getColumnName(21));
        Assertions.assertEquals("SOURCE_DATA_TYPE", columnsMetadata.getColumnName(22));
        Assertions.assertEquals("IS_AUTOINCREMENT", columnsMetadata.getColumnName(23));
        Assertions.assertEquals("IS_GENERATEDCOLUMN", columnsMetadata.getColumnName(24));
    }

    /**
     * Tests columns of getColumnPrivileges.
     */
    @Test
    @DisplayName("Tests the correct columns of getColumnPrivileges.")
    void testGetColumnPrivileges() throws SQLException {
        final ResultSet columnPrivileges = metadata.getColumnPrivileges(null, null, null, null);
        final ResultSetMetaData columnPrivilegesMetadata = columnPrivileges.getMetaData();

        Assertions.assertEquals("TABLE_CAT", columnPrivilegesMetadata.getColumnName(1));
        Assertions.assertEquals("TABLE_SCHEM", columnPrivilegesMetadata.getColumnName(2));
        Assertions.assertEquals("TABLE_NAME", columnPrivilegesMetadata.getColumnName(3));
        Assertions.assertEquals("COLUMN_NAME", columnPrivilegesMetadata.getColumnName(4));
        Assertions.assertEquals("GRANTOR", columnPrivilegesMetadata.getColumnName(5));
        Assertions.assertEquals("GRANTEE", columnPrivilegesMetadata.getColumnName(6));
        Assertions.assertEquals("PRIVILEGE", columnPrivilegesMetadata.getColumnName(7));
        Assertions.assertEquals("IS_GRANTABLE", columnPrivilegesMetadata.getColumnName(8));
    }

    /**
     * Tests columns of getPrimaryKeys().
     */
    @Test
    @DisplayName("Tests the correct columns of getPrimaryKeys.")
    void testGetPrimaryKeys() throws SQLException {
        final ResultSet primaryKeys = metadata.getPrimaryKeys(null, null, COLLECTION_NAME);
        final ResultSetMetaData primaryKeysMetadata = primaryKeys.getMetaData();
        Assertions.assertEquals("TABLE_CAT", primaryKeysMetadata.getColumnName(1));
        Assertions.assertEquals("TABLE_SCHEM", primaryKeysMetadata.getColumnName(2));
        Assertions.assertEquals("TABLE_NAME", primaryKeysMetadata.getColumnName(3));
        Assertions.assertEquals("COLUMN_NAME", primaryKeysMetadata.getColumnName(4));
        Assertions.assertEquals("KEY_SEQ", primaryKeysMetadata.getColumnName(5));
        Assertions.assertEquals("PK_NAME", primaryKeysMetadata.getColumnName(6));
    }

    /**
     * Tests columns of getImportedKeys()
     */
    @Test
    @DisplayName("Tests the correct columns of foreign keys.")
    void testGetImportedKeys() throws SQLException {
        final ResultSet importedKeys = metadata.getImportedKeys(null, null, COLLECTION_NAME);
        final ResultSetMetaData foreignKeysMetadata = importedKeys.getMetaData();
        Assertions.assertEquals("PKTABLE_CAT", foreignKeysMetadata.getColumnName(1));
        Assertions.assertEquals("PKTABLE_SCHEM", foreignKeysMetadata.getColumnName(2));
        Assertions.assertEquals("PKTABLE_NAME", foreignKeysMetadata.getColumnName(3));
        Assertions.assertEquals("PKCOLUMN_NAME", foreignKeysMetadata.getColumnName(4));
        Assertions.assertEquals("FKTABLE_CAT", foreignKeysMetadata.getColumnName(5));
        Assertions.assertEquals("FKTABLE_SCHEM", foreignKeysMetadata.getColumnName(6));
        Assertions.assertEquals("FKTABLE_NAME", foreignKeysMetadata.getColumnName(7));
        Assertions.assertEquals("FKCOLUMN_NAME", foreignKeysMetadata.getColumnName(8));
        Assertions.assertEquals("KEY_SEQ", foreignKeysMetadata.getColumnName(9));
        Assertions.assertEquals("UPDATE_RULE", foreignKeysMetadata.getColumnName(10));
        Assertions.assertEquals("DELETE_RULE", foreignKeysMetadata.getColumnName(11));
        Assertions.assertEquals("FK_NAME", foreignKeysMetadata.getColumnName(12));
        Assertions.assertEquals("PK_NAME", foreignKeysMetadata.getColumnName(13));
        Assertions.assertEquals("DEFERRABILITY", foreignKeysMetadata.getColumnName(14));
    }

    /**
     * Tests columns of getAttributes().
     */
    @Test
    @DisplayName("Tests the correct columns of getAttributes.")
    void testGetAttributes() throws SQLException {
        final ResultSet attributes = metadata.getAttributes(null,
                null, null, null);
        final ResultSetMetaData attributesMetadata = attributes.getMetaData();
        Assertions.assertEquals("TYPE_CAT", attributesMetadata.getColumnName(1));
        Assertions.assertEquals("TYPE_SCHEM", attributesMetadata.getColumnName(2));
        Assertions.assertEquals("TYPE_NAME", attributesMetadata.getColumnName(3));
        Assertions.assertEquals("ATTR_NAME", attributesMetadata.getColumnName(4));
        Assertions.assertEquals("DATA_TYPE", attributesMetadata.getColumnName(5));
        Assertions.assertEquals("ATTR_TYPE_NAME", attributesMetadata.getColumnName(6));
        Assertions.assertEquals("ATTR_SIZE", attributesMetadata.getColumnName(7));
        Assertions.assertEquals("DECIMAL_DIGITS", attributesMetadata.getColumnName(8));
        Assertions.assertEquals("NUM_PREC_RADIX", attributesMetadata.getColumnName(9));
        Assertions.assertEquals("NULLABLE", attributesMetadata.getColumnName(10));
        Assertions.assertEquals("REMARKS", attributesMetadata.getColumnName(11));
        Assertions.assertEquals("ATTR_DEF", attributesMetadata.getColumnName(12));
        Assertions.assertEquals("SQL_DATA_TYPE", attributesMetadata.getColumnName(13));
        Assertions.assertEquals("SQL_DATETIME_SUB", attributesMetadata.getColumnName(14));
        Assertions.assertEquals("CHAR_OCTET_LENGTH", attributesMetadata.getColumnName(15));
        Assertions.assertEquals("ORDINAL_POSITION", attributesMetadata.getColumnName(16));
        Assertions.assertEquals("IS_NULLABLE", attributesMetadata.getColumnName(17));
        Assertions.assertEquals("SCOPE_CATALOG", attributesMetadata.getColumnName(18));
        Assertions.assertEquals("SCOPE_SCHEMA", attributesMetadata.getColumnName(19));
        Assertions.assertEquals("SCOPE_TABLE", attributesMetadata.getColumnName(20));
        Assertions.assertEquals("SOURCE_DATA_TYPE", attributesMetadata.getColumnName(21));
    }
}
