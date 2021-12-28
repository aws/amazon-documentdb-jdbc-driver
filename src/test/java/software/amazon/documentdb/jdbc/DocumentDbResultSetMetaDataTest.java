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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbResultSetMetaDataTest extends DocumentDbFlapDoodleTest {

    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "testDb";
    private static final String HOSTNAME = "localhost";
    private static final String COLLECTION_SIMPLE = "COLLECTION_SIMPLE";
    private static final String COLLECTION_COMPLEX = "COLLECTION_COMPLEX";
    private static final String CONNECTION_STRING_TEMPLATE = "jdbc:documentdb://%s:%s@%s:%s/%s?tls=false";

    /** Initializes the test class. */
    @BeforeAll
    void initialize() {
        // Add 1 valid user so we can successfully authenticate.
        createUser(DATABASE, USERNAME, PASSWORD);
        prepareSimpleConsistentData(DATABASE, COLLECTION_SIMPLE, 5, USERNAME, PASSWORD);
        addComplexData();
    }

    @AfterEach
    void afterEach() throws Exception {
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(new Properties(),
                        getJdbcConnectionString(),
                "jdbc:documentdb:");
        try (DocumentDbSchemaWriter schemaWriter = new DocumentDbSchemaWriter(properties, null)) {
            schemaWriter.remove(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
        }
    }

    /**
     * Tests resultSet.getMetadata
     * @throws SQLException if connection fails.
     */
    @Test
    @DisplayName("Tests metadata of a database with simple data.")
    void testGetResultSetMetadataSimple() throws SQLException {
        final String connectionString = getJdbcConnectionString();

        try (final Connection connection = DriverManager.getConnection(connectionString);
                final DocumentDbStatement statement = (DocumentDbStatement) connection.createStatement();
                final ResultSet resultSet =
                        statement.executeQuery(String.format("SELECT * FROM \"%s\"", COLLECTION_SIMPLE))) {
            final ResultSetMetaData metadata = resultSet.getMetaData();
            Assertions.assertEquals(13, metadata.getColumnCount());
            Assertions.assertEquals(COLLECTION_SIMPLE, metadata.getTableName(1));
            Assertions.assertNull(metadata.getCatalogName(1));
            Assertions.assertEquals(DATABASE, metadata.getSchemaName(1));

            Assertions.assertEquals(COLLECTION_SIMPLE + "__id", metadata.getColumnName(1));
            Assertions.assertEquals(COLLECTION_SIMPLE + "__id", metadata.getColumnLabel(1));
            Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(1));
            Assertions.assertEquals("java.lang.String", metadata.getColumnClassName(1));
            Assertions.assertEquals(Types.VARCHAR, metadata.getColumnType(1));
            Assertions.assertEquals(0, metadata.isNullable(1));
            Assertions.assertEquals(65536, metadata.getPrecision(1));
            Assertions.assertEquals(65536, metadata.getColumnDisplaySize(1));

            Assertions.assertTrue(metadata.isReadOnly(1));
            Assertions.assertTrue(metadata.isSigned(1));
            Assertions.assertTrue(metadata.isCaseSensitive(1));
            Assertions.assertFalse(metadata.isSearchable(1));
            Assertions.assertFalse(metadata.isWritable(1));
            Assertions.assertFalse(metadata.isAutoIncrement(1));
            Assertions.assertFalse(metadata.isCurrency(1));
            Assertions.assertFalse(metadata.isDefinitelyWritable(1));

            Assertions.assertEquals("fieldDouble", metadata.getColumnName(2));
            Assertions.assertEquals("DOUBLE", metadata.getColumnTypeName(2));
            Assertions.assertEquals(1, metadata.isNullable(2));
            Assertions.assertEquals(0, metadata.getScale(2));

            Assertions.assertEquals("fieldString", metadata.getColumnName(3));
            Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(3));

            Assertions.assertEquals("fieldObjectId", metadata.getColumnName(4));
            Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(4));

            Assertions.assertEquals("fieldBoolean", metadata.getColumnName(5));
            Assertions.assertEquals("BOOLEAN", metadata.getColumnTypeName(5));

            Assertions.assertEquals("fieldDate", metadata.getColumnName(6));
            Assertions.assertEquals("TIMESTAMP", metadata.getColumnTypeName(6));

            Assertions.assertEquals("fieldInt", metadata.getColumnName(7));
            Assertions.assertEquals("INTEGER", metadata.getColumnTypeName(7));

            Assertions.assertEquals("fieldLong", metadata.getColumnName(8));
            Assertions.assertEquals("BIGINT", metadata.getColumnTypeName(8));

            Assertions.assertEquals("fieldMaxKey", metadata.getColumnName(9));
            Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(9));

            Assertions.assertEquals("fieldMinKey", metadata.getColumnName(10));
            Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(10));

            Assertions.assertEquals("fieldNull", metadata.getColumnName(11));
            Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(11));

            Assertions.assertEquals("fieldBinary", metadata.getColumnName(12));
            Assertions.assertEquals("VARBINARY", metadata.getColumnTypeName(12));

            Assertions.assertEquals("fieldDecimal128", metadata.getColumnName(13));
            Assertions.assertEquals("DECIMAL", metadata.getColumnTypeName(13));
        }
    }

    /**
     * Test for complex databases
     */
    @Test
    @DisplayName("Tests metadata of a database with nested documents and an array.")
    void testResultSetGetMetadataComplex() throws SQLException {
        final String connectionString = getJdbcConnectionString();
        try (final Connection connection = DriverManager.getConnection(connectionString);
                final DocumentDbStatement statement = (DocumentDbStatement) connection.createStatement();
                final ResultSet outerTableResultSet =
                        statement.executeQuery(String.format("SELECT * FROM \"%s\"", COLLECTION_COMPLEX));
                final ResultSet levelOneNestedTable =
                        statement.executeQuery(
                                String.format("SELECT * FROM \"%s\"", COLLECTION_COMPLEX + "_innerDocument"));
                final ResultSet levelOneNestedTableTwo =
                        statement.executeQuery(
                                String.format("SELECT * FROM \"%s\"", COLLECTION_COMPLEX + "_innerDocumentTwo"));
                final ResultSet levelTwoNestedTable =
                        statement.executeQuery(
                                String.format(
                                        "SELECT * FROM \"%s\"",
                                        COLLECTION_COMPLEX + "_innerDocument_levelTwoDocument"));
                final ResultSet arrayTable =
                        statement.executeQuery(
                                String.format("SELECT * FROM \"%s\"", COLLECTION_COMPLEX + "_array"))) {
            Assertions.assertNotNull(outerTableResultSet);
            final ResultSetMetaData outerMetadata = outerTableResultSet.getMetaData();
            Assertions.assertEquals(2, outerMetadata.getColumnCount());
            Assertions.assertEquals(COLLECTION_COMPLEX + "__id", outerMetadata.getColumnName(1));
            Assertions.assertEquals("count", outerMetadata.getColumnName(2));
            Assertions.assertEquals("VARCHAR", outerMetadata.getColumnTypeName(1));
            Assertions.assertEquals("INTEGER", outerMetadata.getColumnTypeName(2));

            Assertions.assertNotNull(levelOneNestedTable);
            final ResultSetMetaData innerMetadata = levelOneNestedTable.getMetaData();
            Assertions.assertEquals(2, innerMetadata.getColumnCount());
            Assertions.assertEquals(COLLECTION_COMPLEX + "__id", innerMetadata.getColumnName(1));
            Assertions.assertEquals("levelOneString", innerMetadata.getColumnName(2));
            Assertions.assertEquals("VARCHAR", innerMetadata.getColumnTypeName(1));
            Assertions.assertEquals("VARCHAR", innerMetadata.getColumnTypeName(2));

            Assertions.assertNotNull(levelOneNestedTableTwo);
            final ResultSetMetaData innerMetadataTwo = levelOneNestedTableTwo.getMetaData();
            Assertions.assertEquals(2, innerMetadataTwo.getColumnCount());
            Assertions.assertEquals(COLLECTION_COMPLEX + "__id", innerMetadata.getColumnName(1));
            Assertions.assertEquals("levelOneInt", innerMetadataTwo.getColumnName(2));
            Assertions.assertEquals("VARCHAR", innerMetadataTwo.getColumnTypeName(1));
            Assertions.assertEquals("INTEGER", innerMetadataTwo.getColumnTypeName(2));

            Assertions.assertNotNull(levelTwoNestedTable);
            final ResultSetMetaData levelTwoMetadata = levelTwoNestedTable.getMetaData();
            Assertions.assertEquals(3, levelTwoMetadata.getColumnCount());
            Assertions.assertEquals(COLLECTION_COMPLEX + "__id", levelTwoMetadata.getColumnName(1));
            Assertions.assertEquals("levelTwoInt", levelTwoMetadata.getColumnName(2));
            Assertions.assertEquals("levelTwoField", levelTwoMetadata.getColumnName(3));
            Assertions.assertEquals("VARCHAR", levelTwoMetadata.getColumnTypeName(1));
            Assertions.assertEquals("INTEGER", levelTwoMetadata.getColumnTypeName(2));
            Assertions.assertEquals("VARCHAR", levelTwoMetadata.getColumnTypeName(3));

            Assertions.assertNotNull(arrayTable);
            final ResultSetMetaData arrayMetadata = arrayTable.getMetaData();
            Assertions.assertEquals(3, arrayMetadata.getColumnCount());
            Assertions.assertEquals(COLLECTION_COMPLEX + "__id", arrayMetadata.getColumnName(1));
            Assertions.assertEquals("array_index_lvl_0", arrayMetadata.getColumnName(2));
            Assertions.assertEquals("value", arrayMetadata.getColumnName(3));
            Assertions.assertEquals("VARCHAR", arrayMetadata.getColumnTypeName(1));
            Assertions.assertEquals("BIGINT", arrayMetadata.getColumnTypeName(2));
            Assertions.assertEquals("INTEGER", arrayMetadata.getColumnTypeName(3));
        }
    }

    /**
     * Test for complex databases
     */
    @Test
    @DisplayName("Tests attempting to retrieve metadata with invalid indices.")
    void testResultSetGetMetadataInvalidIndices() throws SQLException {
        final String connectionString = getJdbcConnectionString();
        try (final Connection connection = DriverManager.getConnection(connectionString);
                final DocumentDbStatement statement = (DocumentDbStatement) connection.createStatement();
                final ResultSet resultSet =
                        statement.executeQuery(String.format("SELECT * FROM \"%s\"", COLLECTION_SIMPLE))) {
            final ResultSetMetaData metadata = resultSet.getMetaData();
            Assertions.assertEquals(13, metadata.getColumnCount());
            // Attempt to get 0th column.
            Assertions.assertEquals(
                    SqlError.lookup(SqlError.INVALID_INDEX, 0, 13),
                    Assertions.assertThrows(SQLException.class, () -> metadata.getColumnName(0))
                            .getMessage());
            // Attempt to get 14th column.
            Assertions.assertEquals(
                    SqlError.lookup(SqlError.INVALID_INDEX, 14, 13),
                    Assertions.assertThrows(SQLException.class, () -> metadata.getColumnName(14))
                            .getMessage());
        }
    }

    private String getJdbcConnectionString() {
        return String.format(
                CONNECTION_STRING_TEMPLATE, USERNAME, PASSWORD, HOSTNAME, getMongoPort(), DATABASE);
    }

    /**
     * Adds data with second level nested documents as well as an array
     */
    private void addComplexData() {
        final MongoClient client = createMongoClient("admin", USERNAME, PASSWORD);

        final MongoDatabase database = client.getDatabase(DATABASE);

        final MongoCollection<BsonDocument> collection = database.getCollection(COLLECTION_COMPLEX, BsonDocument.class);
        for (int count = 0; count < 5; count++) {
            final BsonDocument levelTwoDocument = new BsonDocument()
                    .append("levelTwoInt", new BsonInt32(2))
                    .append("levelTwoField", new BsonString("string"));
            final BsonDocument innerDocument = new BsonDocument()
                    .append("levelOneString", new BsonString("levelOne"))
                    .append("levelTwoDocument", levelTwoDocument);
            final BsonDocument innerDocumentTwo = new BsonDocument()
                    .append("levelOneInt", new BsonInt32(2));
            final BsonArray array = new BsonArray();
            array.add(new BsonInt32(3));
            array.add(new BsonInt32(4));

            final BsonDocument outerDocument = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("count", new BsonInt32(count))
                    .append("innerDocument", innerDocument)
                    .append("innerDocumentTwo", innerDocumentTwo)
                    .append("array", array);
            final InsertOneResult result = collection.insertOne(outerDocument);
            Assertions.assertEquals(count + 1, collection.countDocuments());
            Assertions.assertEquals(outerDocument.getObjectId("_id"), result.getInsertedId());
        }
    }
}
