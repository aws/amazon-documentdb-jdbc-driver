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

import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.common.utilities.JdbcType;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbDatabaseMetaDataTest extends DocumentDbFlapDoodleTest {

    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "testDb";
    private static final String COLLECTION_BASIC = "COLLECTION";
    private static final String COLLECTION_SUB = "collectionSubDocument";
    private static final String COLLECTION_ARRAY = "collectionWithArray";
    private static final String HOSTNAME = "localhost";
    private static final String DRIVER_MAJOR_VERSION_KEY = "driver.major.version";
    private static final String DRIVER_MINOR_VERSION_KEY = "driver.minor.version";
    private static final String DRIVER_FULL_VERSION_KEY = "driver.full.version";
    private static final String PROPERTIES_FILE_PATH = "/project.properties";

    private static Connection connection;
    private static DatabaseMetaData metadata;

    /** Initializes the test class. */
    @BeforeAll
    public static void initialize() throws SQLException {
        createUser(DATABASE, USERNAME, PASSWORD);
        prepareSimpleConsistentData(DATABASE, COLLECTION_BASIC,
                5, USERNAME, PASSWORD);
        final MongoClient client = createMongoClient(ADMIN_DATABASE, USERNAME, PASSWORD);
        final Document nestedDocument = Document.parse(
                "{ \"_id\" : \"key\", " +
                        "\"doc\" : { \"field\" : 1 } }");
        client.getDatabase(DATABASE).getCollection(COLLECTION_SUB).insertOne(nestedDocument);
        final Document arrayDocument = Document.parse(
            "{\n" +
                    "  \"_id\":3,\n" +
                    "  \"field\":\"string\",\n" +
                    "  \"array\": [\n" +
                    "    1, 2, 3\n" +
                    "  ]\n" +
                    "}"
        );
        client.getDatabase(DATABASE).getCollection(COLLECTION_ARRAY).insertOne(arrayDocument);
        final String connectionString = String.format(
                "jdbc:documentdb://%s:%s@%s:%s/%s?tls=false", USERNAME, PASSWORD, HOSTNAME, getMongoPort(), DATABASE);
        connection = DriverManager.getConnection(connectionString);
        metadata = connection.getMetaData();
    }

    @AfterAll
    static void afterAll() throws Exception {
        final Properties info = connection.getClientInfo();
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info,
                        "jdbc:documentdb:", "jdbc:documentdb:");
        try (DocumentDbSchemaWriter schemaWriter = new DocumentDbSchemaWriter(properties, null)) {
            schemaWriter.remove(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
        }
        connection.close();
    }

    /**
     * Tests for basic metadata fields.
     */
    @Test
    @DisplayName("Tests basic common properties of a database.")
    void testBasicMetadata() throws SQLException, IOException {
        // Retrieve the version metadata from properties file.
        final int majorVersion;
        final int minorVersion;
        final String fullVersion;
        try (InputStream is = DocumentDbDatabaseMetaData.class.getResourceAsStream(PROPERTIES_FILE_PATH)) {
            final Properties p = new Properties();
            p.load(is);
            majorVersion = Integer.parseInt(p.getProperty(DRIVER_MAJOR_VERSION_KEY));
            minorVersion = Integer.parseInt(p.getProperty(DRIVER_MINOR_VERSION_KEY));
            fullVersion = p.getProperty(DRIVER_FULL_VERSION_KEY);
        }
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
        Assertions.assertEquals(majorVersion, metadata.getDriverMajorVersion());
        Assertions.assertEquals(minorVersion, metadata.getDriverMinorVersion());
        Assertions.assertEquals(fullVersion, metadata.getDriverVersion());
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
        // Catalog pattern, schema pattern, table name pattern, table types
        final String[][] tests = new String [][]{
                {null, null, null, null},
                {" ", " ", " ", " "},
                {null, null, null, "TABLE"},
                {null, null, COLLECTION_BASIC, "TABLE"},
                {"", null, COLLECTION_BASIC, "TABLE"},
                {" ", null, COLLECTION_BASIC, " "},
                {null, DATABASE, COLLECTION_BASIC, "TABLE"},
                {" ", DATABASE, COLLECTION_BASIC, "TABLE"},
                {"non-existing catalog", DATABASE, COLLECTION_BASIC, "non-existing table"},
                {null, null, null},
                {" ", " ", " "}
        };

        for (String[] test : tests) {
            final String[] tableTypes = test.length == 4 ? new String[]{test[3]} : new String[]{};
            final ResultSet tables = metadata.getTables(test[0], test[1], test[2], tableTypes);
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
    }

    /**
     * Test getTables returns empty ResultSet.
     */
    @Test
    @DisplayName("Test getTables returns empty ResultSet.")
    void testGetMetadataTablesEmpty() throws SQLException {
        // Catalog pattern, schema pattern, table name pattern, table types
        final String[][] tests = new String [][]{
                {null, "", COLLECTION_BASIC, "TABLE"},
                {" ", "", COLLECTION_BASIC, "TABLE"},
                {"non-existing catalog", DATABASE, COLLECTION_BASIC, "non-existing table"},
                {" ", "", COLLECTION_BASIC},
                {null, "", COLLECTION_BASIC},
        };
        for (String[] test : tests) {
            final String[] tableTypes = test.length == 4 ? new String[]{test[3]} : null;
            final ResultSet tables = metadata.getTables(test[0], test[1], test[2], tableTypes);
            Assertions.assertFalse(tables.next());
        }
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
        // Catalog pattern, schema pattern, table pattern, column pattern
        final String[][] tests = new String [][]{
                {null, null, null, null},
                {" ", " ", " ", " "},
                {null, null, null, "%__id"},
                {null, "%", null, "%__id"},
                {null, "%", "%", "%__id"},
                {"", null, COLLECTION_BASIC, "%__id"},
                {null, DATABASE, COLLECTION_BASIC, "%__id"},
                {"", null, COLLECTION_BASIC, "%\\_\\_id"},
        };
        for (String[] test : tests) {
            final ResultSet columns = metadata.getColumns(test[0], test[1], test[2], test[3]);
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
    }

    /**
     * Test getColumns returns empty ResultSet.
     */
    @Test
    @DisplayName("Test getColumns returns empty ResultSet.")
    void testGetMetadataColumnsEmpty() throws SQLException {
        // Catalog pattern, schema pattern, table pattern, column pattern
        final String[][] tests = new String [][]{
                {null, "", COLLECTION_BASIC, "%__id"},
                {" ", "", COLLECTION_BASIC, "%__id"},
                {"non-existent catalog", DATABASE, COLLECTION_BASIC, "%\\_\\_id"},
                {"non-existent catalog", null, COLLECTION_BASIC, "non-existent column"}
        };
        for (String[] test : tests) {
            final ResultSet columns = metadata.getColumns(test[0], test[1], test[2], test[3]);
            Assertions.assertFalse(columns.next());
        }
    }

    /**
     * Tests that convertPatternToRegex works as expected.
     */
    @Test
    @DisplayName("Tests that convertPatternToRegex works as expected.")
    void testConvertPattern() {
        // Test input, expected result
        final String[][] tests = new String [][]{
                {null, ""},
                {"", ""},
                {" ", ""},
                {"_", "."},
                {"_b", ".\\Qb\\E"},
                {"a_b", "\\Qa\\E.\\Qb\\E"},
                {"a_", "\\Qa\\E."},
                {"%", ".*"},
                {"a%b", "\\Qa\\E.*\\Qb\\E"},
                {"%b", ".*\\Qb\\E"},
                {"a%", "\\Qa\\E.*"},
                {"\\_", "[_]"},
                {"\\%", "[%]"},
                {"\\\\", "[\\]"},
                {"\\_\\%\\\\", "[_][%][\\]"},
                {"\\_\\%", "[_][%]"},
                {"a\\_b\\%c", "\\Qa\\E[_]\\Qb\\E[%]\\Qc\\E"},
                {"a_b%c", "\\Qa\\E.\\Qb\\E.*\\Qc\\E"},
                {"\\_\\\\%", "[_][\\].*"},
                {"\\", "\\Q\\\\E"}, // mis-balanced escape
                {"\\\\\\\\\\", "[\\][\\]\\Q\\\\E"}, // mis-balanced escape
                {"\\_\\%\\", "[_][%]\\Q\\\\E"}, // mis-balanced escape
        };

        for (String[] test : tests) {
            Assertions.assertEquals(test[1], DocumentDbDatabaseMetaData.convertPatternToRegex(test[0]));
        }
    }

    /**
     * Tests columns of getColumnPrivileges.
     */
    @Test
    @DisplayName("Tests the correct columns of getColumnPrivileges.")
    void testGetColumnPrivileges() throws SQLException {
        // Catalog pattern, schema pattern, table pattern, column pattern
        final String[][] tests = new String [][]{
                {null, null, null, null},
                {" ", " ", " ", " "},
                {null, "%", null, null},
                {null, null, null, "%__id"},
                {null, null, COLLECTION_BASIC, "%__id"},
                {null, DATABASE, COLLECTION_BASIC, "%__id"}
        };

        for (String[] test : tests) {
            final ResultSet columnPrivileges = metadata.getColumnPrivileges(test[0], test[1], test[2], test[3]);
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
    }

    @Test
    @DisplayName("Tests single table type TABLE is returned from getTableTypes.")
    void testGetTableTypes() throws SQLException {
        final ResultSet tableTypes = metadata.getTableTypes();
        final ResultSetMetaData tableTypesMetadata = tableTypes.getMetaData();
        Assertions.assertEquals(1, tableTypesMetadata.getColumnCount());
        Assertions.assertEquals("TABLE_TYPE", tableTypesMetadata.getColumnName(1));
        Assertions.assertEquals("TABLE_TYPE", tableTypesMetadata.getColumnLabel(1));
        Assertions.assertEquals(DATABASE, tableTypesMetadata.getSchemaName(1));
        Assertions.assertEquals(0,tableTypesMetadata.isNullable(1));
        Assertions.assertEquals(0,tableTypesMetadata.getPrecision(1));
        Assertions.assertEquals(0,tableTypesMetadata.getScale(1));
        Assertions.assertEquals(64,tableTypesMetadata.getColumnDisplaySize(1));
        Assertions.assertEquals(Types.VARCHAR, tableTypesMetadata.getColumnType(1));
        Assertions.assertEquals(JdbcType.VARCHAR.name(), tableTypesMetadata.getColumnTypeName(1));
        Assertions.assertTrue(tableTypes.next());
        Assertions.assertEquals("TABLE", tableTypes.getString(1));
        Assertions.assertFalse(tableTypes.next());
    }

    /**
     * Tests columns of getPrimaryKeys().
     */
    @Test
    @DisplayName("Tests the correct columns of getPrimaryKeys.")
    void testGetPrimaryKeys() throws SQLException {
        final ResultSet primaryKeys = metadata.getPrimaryKeys(null, null, COLLECTION_BASIC);
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
        final ResultSet importedKeys = metadata.getImportedKeys(null, null, COLLECTION_BASIC);
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

    @Test
    @DisplayName("Tests basic primary key metadata.")
    void testGetPrimaryKeyBasic() throws SQLException {
        final ResultSet primaryKeys = metadata.getPrimaryKeys(null, DATABASE, COLLECTION_BASIC);
        Assertions.assertNotNull(primaryKeys);
        Assertions.assertTrue(primaryKeys.next());
        Assertions.assertNull(primaryKeys.getString(1));
        Assertions.assertEquals(DATABASE, primaryKeys.getString(2));
        Assertions.assertEquals(COLLECTION_BASIC, primaryKeys.getString(3));
        Assertions.assertEquals(COLLECTION_BASIC + "__id", primaryKeys.getString(4));
        Assertions.assertEquals(1, primaryKeys.getShort(5));
        Assertions.assertNull(primaryKeys.getString(6));
        Assertions.assertFalse(primaryKeys.next());
    }

    @Test
    @DisplayName("Tests primary keys of sub-document virtual table.")
    void testGetPrimaryKeySubdocument() throws SQLException {
        final ResultSet primaryKeys = metadata.getPrimaryKeys(null, DATABASE, COLLECTION_SUB + "_doc");
        Assertions.assertNotNull(primaryKeys);
        Assertions.assertTrue(primaryKeys.next());
        Assertions.assertNull(primaryKeys.getString(1));
        Assertions.assertEquals(DATABASE, primaryKeys.getString(2));
        Assertions.assertEquals(COLLECTION_SUB + "_doc", primaryKeys.getString(3));
        Assertions.assertEquals(COLLECTION_SUB + "__id", primaryKeys.getString(4));
        Assertions.assertEquals(1, primaryKeys.getShort(5));
        Assertions.assertNull(primaryKeys.getString(6));
        Assertions.assertFalse(primaryKeys.next());
    }

    @Test
    @DisplayName("Tests that filtering by schema and table works on getPrimaryKeys.")
    void testGetPrimaryKeysFilters() throws SQLException {
        final ResultSet emptyResultSetSchema = metadata.getPrimaryKeys(null, "invalidDb", null);
        Assertions.assertFalse(emptyResultSetSchema.next());
        final ResultSet emptyResultSetTable = metadata.getPrimaryKeys(null, null, "invalidCollection");
        Assertions.assertFalse(emptyResultSetTable.next());
        final ResultSet noFilterPrimaryKeys = metadata.getPrimaryKeys(null, null, null);
        Assertions.assertTrue(noFilterPrimaryKeys.next());
    }

    @Test
    @DisplayName("Tests that filtering by schema and table works on getImportedKeys.")
    void testGetImportedKeysFilters() throws SQLException {
        final String[][] tests = new String [][]{
                {null, "invalidDb", null},
                {null, null, "invalidCollection"},
                {null, DATABASE, "invalidCollection"},
                {"invalidCatalog", null, COLLECTION_SUB + "_doc"},
                {null, null, "invalidCollection"},
                {"invalidCatalog", "invalidDb", COLLECTION_SUB + "_doc"},
        };
        for (String[] test : tests) {
            final ResultSet emptyResultSetSchema = metadata.getImportedKeys(test[0], test[1], test[2]);
            Assertions.assertFalse(emptyResultSetSchema.next());
        }

        final ResultSet noFilterImportedKeys = metadata.getImportedKeys(null, null, null);
        Assertions.assertTrue(noFilterImportedKeys.next());

    }

    @Test
    @DisplayName("Tests primary/foreign key queries with wildcard characters (% and _)")
    void testPrimaryKeyWildcards() throws SQLException {
        final ResultSet primaryWildcardKeys = metadata.getPrimaryKeys(null, null, "_ollect%");
        Assertions.assertTrue(primaryWildcardKeys.next());
        final ResultSet foreignWildcardKeys = metadata.getImportedKeys(null, null, "_ollect%");
        Assertions.assertTrue(foreignWildcardKeys.next());
    }

    @Test
    @DisplayName("Tests primary keys of array virtual tables.")
    void testGetPrimaryKeysArray() throws SQLException {
        final ResultSet arrayPrimaryKeys = metadata.getPrimaryKeys(null, null, COLLECTION_ARRAY + "_array");
        Assertions.assertTrue(arrayPrimaryKeys.next());
        Assertions.assertNull(arrayPrimaryKeys.getString(1));
        Assertions.assertEquals(DATABASE, arrayPrimaryKeys.getString(2));
        Assertions.assertEquals(COLLECTION_ARRAY + "_array", arrayPrimaryKeys.getString(3));
        Assertions.assertEquals(COLLECTION_ARRAY + "__id", arrayPrimaryKeys.getString(4));
        Assertions.assertEquals(1, arrayPrimaryKeys.getShort(5));
        Assertions.assertNull(arrayPrimaryKeys.getString(6));
        Assertions.assertTrue(arrayPrimaryKeys.next());
        Assertions.assertNull(arrayPrimaryKeys.getString(1));
        Assertions.assertEquals(DATABASE, arrayPrimaryKeys.getString(2));
        Assertions.assertEquals(COLLECTION_ARRAY + "_array", arrayPrimaryKeys.getString(3));
        Assertions.assertEquals("array_index_lvl_0", arrayPrimaryKeys.getString(4));
        Assertions.assertEquals(2, arrayPrimaryKeys.getShort(5)); // Indicates second column of PK
        Assertions.assertNull(arrayPrimaryKeys.getString(6));
        Assertions.assertFalse(arrayPrimaryKeys.next());
    }

    @Test
    @DisplayName("Tests foreign keys of sub-document virtual tables.")
    void testGetImportedKeysDocument() throws SQLException {
        final ResultSet subdocImportedKeys = metadata.getImportedKeys(null, null, COLLECTION_SUB + "_doc");
        Assertions.assertTrue(subdocImportedKeys.next());
        Assertions.assertNull(subdocImportedKeys.getString(1));
        Assertions.assertEquals(DATABASE, subdocImportedKeys.getString(2));
        Assertions.assertEquals(COLLECTION_SUB, subdocImportedKeys.getString(3));
        Assertions.assertEquals(COLLECTION_SUB + "__id", subdocImportedKeys.getString(4));
        Assertions.assertNull(subdocImportedKeys.getString(5));
        Assertions.assertEquals(DATABASE, subdocImportedKeys.getString(6));
        Assertions.assertEquals(COLLECTION_SUB + "_doc", subdocImportedKeys.getString(7));
        Assertions.assertEquals(COLLECTION_SUB + "__id", subdocImportedKeys.getString(8));
        Assertions.assertEquals(1, subdocImportedKeys.getShort(9));
        Assertions.assertFalse(subdocImportedKeys.next());
    }

    @Test
    @DisplayName("Tests foreign keys of array virtual tables.")
    void testGetImportedKeysArray() throws SQLException {
        final ResultSet arrayImportedKeys = metadata.getImportedKeys(null, null, COLLECTION_ARRAY + "_array");
        Assertions.assertTrue(arrayImportedKeys.next());
        Assertions.assertNull(arrayImportedKeys.getString(1));
        Assertions.assertEquals(DATABASE, arrayImportedKeys.getString(2));
        Assertions.assertEquals(COLLECTION_ARRAY, arrayImportedKeys.getString(3));
        Assertions.assertEquals(COLLECTION_ARRAY + "__id", arrayImportedKeys.getString(4));
        Assertions.assertNull(arrayImportedKeys.getString(5));
        Assertions.assertEquals(DATABASE, arrayImportedKeys.getString(6));
        Assertions.assertEquals(COLLECTION_ARRAY + "_array", arrayImportedKeys.getString(7));
        Assertions.assertEquals(COLLECTION_ARRAY + "__id", arrayImportedKeys.getString(8));
        Assertions.assertEquals(1, arrayImportedKeys.getShort(9));
        Assertions.assertFalse(arrayImportedKeys.next());
    }
}
