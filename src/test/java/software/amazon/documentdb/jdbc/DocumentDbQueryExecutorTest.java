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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseMetadata;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static software.amazon.documentdb.jdbc.DocumentDbStatementTest.getDocumentDbStatement;
import static software.amazon.documentdb.jdbc.DocumentDbStatementTest.insertBsonDocuments;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryExecutorTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String TEST_USER = "user";
    private static final String TEST_PASSWORD = "password";
    private static final DocumentDbConnectionProperties VALID_CONNECTION_PROPERTIES = new DocumentDbConnectionProperties();

    @BeforeAll
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only")
    static void initialize() {

        // Add a valid users to the local MongoDB instance.
        createUser(DATABASE_NAME, TEST_USER, TEST_PASSWORD);
        VALID_CONNECTION_PROPERTIES.setUser(TEST_USER);
        VALID_CONNECTION_PROPERTIES.setPassword(TEST_PASSWORD);
        VALID_CONNECTION_PROPERTIES.setDatabase(DATABASE_NAME);
        VALID_CONNECTION_PROPERTIES.setTlsEnabled("false");
        VALID_CONNECTION_PROPERTIES.setHostname("localhost:" + getMongoPort());
    }

    /**
     * Shows that we can get the aggregation operations needed to execute a query.
     */
    @Test
    void testQueryMappingPOC() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        insertBsonDocuments("testQueryMappingPOC", new BsonDocument[]{document});

        final DocumentDbDatabaseMetadata databaseMetadata = DocumentDbDatabaseMetadata.get(
                "id",
                VALID_CONNECTION_PROPERTIES, true);
        final DocumentDbQueryMapper queryMapper = new DocumentDbQueryMapper(VALID_CONNECTION_PROPERTIES,
                databaseMetadata);

        // Get the base table.
        final String basicQuery = String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testQueryMappingPOC");
        DocumentDbMqlQueryContext result = queryMapper.getMqlQueryContext(basicQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("testQueryMappingPOC", result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(0, result.getAggregateOperations().size());

        // Get the nested table.
        final String nestedTableQuery = String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testQueryMappingPOC_array");
        result = queryMapper.getMqlQueryContext(nestedTableQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("testQueryMappingPOC", result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(2, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"
                ),
                result.getAggregateOperations().get(0));

        // Verify WHERE on the nested table to produce only rows where field = 2.
        final String whereQuery = String.format(
                                "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = 2", DATABASE_NAME,
                                "testQueryMappingPOC_array");
        result = queryMapper.getMqlQueryContext(whereQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("testQueryMappingPOC", result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"
                ),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array.field\" : 2 } }"
                ),
                result.getAggregateOperations().get(2));

        // Verify JOIN on the base table and nested table to produce 6 columns and 2 rows.
        final String joinQuery = String.format(
                                "SELECT * FROM \"%s\".\"%s\" "
                                        + "INNER JOIN \"%s\".\"%s\" "
                                        + "ON %s = %s",
                                DATABASE_NAME,
                                "testQueryMappingPOC",
                                DATABASE_NAME,
                                "testQueryMappingPOC_array",
                                "\"testQueryMappingPOC\".\"testQueryMappingPOC__id\"",
                                "\"testQueryMappingPOC_array\".\"testQueryMappingPOC__id\"");
        result = queryMapper.getMqlQueryContext(joinQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals("testQueryMappingPOC", result.getCollectionName());
        Assertions.assertEquals(6, result.getColumnMetaData().size());
        Assertions.assertEquals(2, result.getAggregateOperations().size());
    }

    /**
     * Lifted from DocumentDbResultSetTest but uses the DocumentDbQueryExecutor to get a result.
     * Shows that we are getting the correct result set metadata without the Avatica driver.
     */
    @Test
    void testGetResultSetMetadataSimple() throws SQLException {
        final String collectionSimple = "collectionSimple";
        prepareSimpleConsistentData(DATABASE_NAME, collectionSimple,
                5, TEST_USER, TEST_PASSWORD);
        final DocumentDbDatabaseMetadata databaseMetadata = DocumentDbDatabaseMetadata.get(
                "id",
                VALID_CONNECTION_PROPERTIES,
                true);
        final DocumentDbQueryMapper queryMapper = new DocumentDbQueryMapper(VALID_CONNECTION_PROPERTIES,
                databaseMetadata);
        final DocumentDbStatement statement = getDocumentDbStatement();
        final DocumentDbQueryExecutor queryExecutor = new DocumentDbQueryExecutor(statement, null, queryMapper);
        final ResultSet resultSet = queryExecutor.executeQuery(String.format(
                "SELECT * FROM \"%s\"", collectionSimple));
        Assertions.assertNotNull(resultSet);

        final ResultSetMetaData metadata = resultSet.getMetaData();
        Assertions.assertEquals(13, metadata.getColumnCount());
        Assertions.assertEquals(collectionSimple, metadata.getTableName(1));
        Assertions.assertNull(metadata.getCatalogName(1));
        Assertions.assertEquals(DATABASE_NAME, metadata.getSchemaName(1));

        Assertions.assertEquals(collectionSimple + "__id", metadata.getColumnName(1));
        Assertions.assertEquals(collectionSimple + "__id", metadata.getColumnLabel(1));
        Assertions.assertEquals("VARCHAR", metadata.getColumnTypeName(1));
        Assertions.assertEquals("java.lang.String", metadata.getColumnClassName(1));
        Assertions.assertEquals(Types.VARCHAR,  metadata.getColumnType(1));
        Assertions.assertEquals(0, metadata.isNullable(1));
        Assertions.assertEquals(65536, metadata.getPrecision(1));
        Assertions.assertEquals(65536, metadata.getColumnDisplaySize(1));

        Assertions.assertTrue(metadata.isReadOnly(1));
        Assertions.assertTrue(metadata.isSigned(1));
        Assertions.assertTrue(metadata.isCaseSensitive(1));
        Assertions.assertFalse(metadata.isWritable(1));
        Assertions.assertFalse(metadata.isAutoIncrement(1));
        Assertions.assertFalse(metadata.isCurrency(1));

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
