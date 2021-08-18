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

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class DocumentDbStatementBasicTest extends DocumentDbStatementTest {

    /**
     * Tests querying for all data types with all scan methods.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     * @throws IOException  occurs if reading an input stream fails.
     */
    @DisplayName("Tests that all supported data types can be scanned and retrieved.")
    @ParameterizedTest(name = "testQueryWithAllDataTypes - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironmentsForScanMethods"})
    protected void testQueryWithAllDataTypes(final DocumentDbTestEnvironment testEnvironment, final DocumentDbMetadataScanMethod scanMethod) throws SQLException, IOException {
        setTestEnvironment(testEnvironment);
        final String collectionName = "testDocumentDbDriverTest-" + scanMethod.getName();
        final int recordCount = 10;
        prepareSimpleConsistentData(collectionName, recordCount);
        try (Connection connection = getConnection(scanMethod)) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), collectionName));
            Assertions.assertNotNull(resultSet);
            int count = 0;
            while (resultSet.next()) {
                Assertions.assertTrue(
                        Pattern.matches("^\\w+$", resultSet.getString(collectionName + "__id")));
                Assertions.assertEquals(Double.MAX_VALUE, resultSet.getDouble("fieldDouble"));
                Assertions.assertEquals("新年快乐", resultSet.getString("fieldString"));
                Assertions.assertTrue(
                        Pattern.matches("^\\w+$", resultSet.getString("fieldObjectId")));
                Assertions.assertTrue(resultSet.getBoolean("fieldBoolean"));
                Assertions.assertEquals(
                        Instant.parse("2020-01-01T00:00:00.00Z"),
                        resultSet.getTimestamp("fieldDate").toInstant());
                Assertions.assertEquals(Integer.MAX_VALUE, resultSet.getInt("fieldInt"));
                Assertions.assertEquals(Long.MAX_VALUE, resultSet.getLong("fieldLong"));
                Assertions.assertEquals("MaxKey", resultSet.getString("fieldMaxKey"));
                Assertions.assertEquals("MinKey", resultSet.getString("fieldMinKey"));
                Assertions.assertNull(resultSet.getString("fieldNull"));

                // Test for binary/blob types.
                final Blob blob = resultSet.getBlob("fieldBinary");
                final byte[] expectedBytes = new byte[]{0, 1, 2};
                // Note: pos is 1-indexed
                Assertions.assertArrayEquals(
                        expectedBytes, blob.getBytes(1, (int) blob.length()));
                final byte[] actualBytes = new byte[(int) blob.length()];
                Assertions.assertEquals(3,
                        resultSet.getBinaryStream("fieldBinary").read(actualBytes));
                Assertions.assertArrayEquals(expectedBytes, actualBytes);

                count++;
            }
            Assertions.assertEquals(recordCount, count);
        }
    }

    /**
     * Test querying when there is a conflict between an array and a scalar. The column
     * should become VARCHAR.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests querying when there is conflict between an array and a scalar.")
    @ParameterizedTest(name = "testArrayScalarConflict - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testArrayScalarConflict(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key0\", \n" +
                        "  \"array\" : [ {\n" +
                        "    \"field1\" : 1, \n" +
                        "    \"field2\" : 2 \n" +
                        "  } ] \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key1\", \n" +
                        "  \"array\" : [ 1, 2, 3 ] \n" +
                        "}"
        );
        documents.add(document);
        insertBsonDocuments("testArrayScalarConflict", documents.toArray(new BsonDocument[]{}));

        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet = statement.executeQuery(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                    "testArrayScalarConflict_array"));
            for (int i = 0; i < 4; i++) {
                Assertions.assertTrue(resultSet.next());
                final String arrayValue = resultSet.getString("value");
                if (i == 0) {
                    Assertions.assertEquals("{\"field1\": 1, \"field2\": 2}", arrayValue);
                } else {
                    Assertions.assertEquals(String.valueOf(i), arrayValue);
                }
            }
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Test querying when there is a conflict between a document field and a scalar. The column
     * should become VARCHAR.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying when there is a conflict between a document field and a scalar.")
    @ParameterizedTest(name = "testDocumentScalarConflict - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testDocumentScalarConflict(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key0\", \n" +
                        "  \"doc\" : {\n" +
                        "    \"field1\" : 1, \n" +
                        "    \"field2\" : 2 \n" +
                        "  } \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key1\", \n" +
                        "  \"doc\" : 1 \n" +
                        "}"
        );
        documents.add(document);
        insertBsonDocuments("testDocumentScalarConflict", documents.toArray(new BsonDocument[]{}));
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                    "testDocumentScalarConflict"));
            for (int i = 0; i < 2; i++) {
                Assertions.assertTrue(resultSet.next());
                final String arrayValue = resultSet.getString("doc");
                if (i == 0) {
                    Assertions.assertEquals("{\"field1\": 1, \"field2\": 2}", arrayValue);
                } else {
                    Assertions.assertEquals(String.valueOf(i), arrayValue);
                }
            }
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Test querying when there is a conflict between a document field and an array. The column
     * should become VARCHAR.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying when there is a conflict between a document field and an array.")
    @ParameterizedTest(name = "testArrayDocumentConflict - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testArrayDocumentConflict(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key0\", \n" +
                        "  \"field\" : {\n" +
                        "    \"field1\" : 1, \n" +
                        "    \"field2\" : 2 \n" +
                        "  } \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key1\", \n" +
                        "  \"field\" : [1, 2, 3, 4] \n" +
                        "}"
        );
        documents.add(document);
        insertBsonDocuments("testArrayDocumentConflict", documents.toArray(new BsonDocument[]{}));
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), "testArrayDocumentConflict"));
            for (int i = 0; i < 2; i++) {
                Assertions.assertTrue(resultSet.next());
                final String arrayValue = resultSet.getString("field");
                if (i == 0) {
                    Assertions.assertEquals("{\"field1\": 1, \"field2\": 2}", arrayValue);
                } else {
                    Assertions.assertEquals("[1, 2, 3, 4]", arrayValue);
                }
            }
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Test querying when there is a conflict between a document field and an array of mixed type. The column
     * should become VARCHAR.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying when there is a conflict between a document field and an array of mixed type.")
    @ParameterizedTest(name = "testDocumentAndArrayOfMixedTypesConflict - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testDocumentAndArrayOfMixedTypesConflict(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key0\", \n" +
                        "  \"field\" : {\n" +
                        "    \"field1\" : 1, \n" +
                        "    \"field2\" : 2 \n" +
                        "  } \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key1\", \n" +
                        "  \"field\" : [\n " +
                        "   {\n" +
                        "      \"field1\" : 1,\n" +
                        "      \"field2\" : 2 \n" +
                        "   },\n " +
                        "   1 ] \n " +
                        "}"
        );
        documents.add(document);
        insertBsonDocuments("testDocumentAndArrayOfMixedTypesConflict", documents.toArray(new BsonDocument[]{}));
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                    "testDocumentAndArrayOfMixedTypesConflict"));
            for (int i = 0; i < 2; i++) {
                Assertions.assertTrue(resultSet.next());
                final String arrayValue = resultSet.getString("field");
                if (i == 0) {
                    Assertions.assertEquals("{\"field1\": 1, \"field2\": 2}", arrayValue);
                } else {
                    Assertions.assertEquals("[{\"field1\": 1, \"field2\": 2}, 1]", arrayValue);
                }
            }
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that documents missing a sub-document do not create null rows.
     */
    @DisplayName("Test that documents not containing a sub-document do not add null rows.")
    @ParameterizedTest(name = "testDocumentWithMissingSubDocument - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testDocumentWithMissingSubDocument(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String collection = "testMissingSubDocumentNotNull";
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key0\", \n" +
                        "  \"name\" : \"withDocument\", \n" +
                        "  \"subDocument\" : {\n" +
                        "    \"field1\" : 1, \n" +
                        "    \"field2\" : 2 \n" +
                        "  } \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key1\", \n" +
                        "  \"name\" : \"withoutDocument\" \n" +
                        "}"
        );
        documents.add(document);
        insertBsonDocuments(collection,documents.toArray(new BsonDocument[]{}));
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            statement.execute(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), collection + "_subDocument"));
            final ResultSet results = statement.getResultSet();
            Assertions.assertTrue(results.next());
            Assertions.assertEquals("key0", results.getString(1));
            Assertions.assertEquals(1, results.getInt(2));
            Assertions.assertEquals(2, results.getInt(3));
            Assertions.assertFalse(results.next(), "Contained unexpected extra row.");
        }
    }

    /**
     * Tests that documents missing a nested sub-document do not create null rows.
     */
    @DisplayName("Test that documents not containing a sub-document do not add null rows.")
    @ParameterizedTest(name = "testDocumentWithMissingNestedSubDocument - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testDocumentWithMissingNestedSubDocument(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String collection = "testMissingNestedSubDocumentNotNull";
        final List<BsonDocument> documents = new ArrayList<>();
        BsonDocument document = BsonDocument.parse(
                "{ \"_id\" : \"key0\", \n" +
                        "  \"name\" : \"withDocument\", \n" +
                        "  \"subDocument\" : {\n" +
                        "    \"field1\" : 1, \n" +
                        "    \"nestedSubDoc\" : {\n" +
                        "       \"nestedField\": 7 \n" +
                        "   } \n" +
                        "  } \n" +
                        "}"
        );
        documents.add(document);
        document = BsonDocument.parse(
                "{ \"_id\" : \"key1\", \n" +
                        "  \"name\" : \"withoutDocument\" \n" +
                        "}"
        );
        documents.add(document);
        insertBsonDocuments(collection, documents.toArray(new BsonDocument[]{}));
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            statement.execute(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                    collection + "_subDocument_nestedSubDoc"));
            final ResultSet results = statement.getResultSet();
            Assertions.assertTrue(results.next());
            Assertions.assertEquals("key0", results.getString(1));
            Assertions.assertEquals(7, results.getInt(2));
            Assertions.assertFalse(results.next(), "Contained unexpected extra row.");
        }
    }

    /**
     * Tests COUNT(columnName) doesn't count null or missing values.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests count('column') works ensuring null/undefined values are not counted")
    @ParameterizedTest(name = "testCountColumnName - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCountColumnName(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCountColumn";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abc\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": null}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103\n}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT COUNT(\"field\") from \"%s\".\"%s\"", getDatabaseName(),
                            tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(1, resultSet.getInt(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that SUM(1) works, equivalent to COUNT(*).
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests query with SUM(1).")
    @ParameterizedTest(name = "testQuerySumOne - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQuerySumOne(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQuerySumOne";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT SUM(1) from \"%s\".\"%s\"", getDatabaseName(),
                            tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(3, resultSet.getInt(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries containing ORDER BY and OFFSET work.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests queries with OFFSET")
    @ParameterizedTest(name = "testQueryOffset - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryOffset(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryOffset";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 1,\n" +
                "\"fieldB\": 2}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": 3,\n" +
                "\"fieldB\": 4}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"fieldA\": 5,\n" +
                "\"fieldB\": 6}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * FROM \"%s\".\"%s\" LIMIT 2 OFFSET 1",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("102", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("103", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries containing IN (c1, c2...) work.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests queries with WHERE [field] IN (...)")
    @ParameterizedTest(name = "testQueryWhereIN - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWhereIN(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWhereIN";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 1,\n" +
                "\"fieldB\": \"abc\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": 3,\n" +
                "\"fieldB\": \"def\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"fieldA\": 5,\n" +
                "\"fieldB\": \"ghi\"}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * FROM \"%s\".\"%s\" WHERE \"fieldA\" IN (1, 5)",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("101", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("103", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
            resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * FROM \"%s\".\"%s\" WHERE \"fieldB\" IN ('abc', 'ghi')",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("101", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("103", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries containing NOT IN (c1, c2...) work.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests queries with WHERE [field] NOT IN (...)")
    @ParameterizedTest(name = "testQueryWhereNotIN - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWhereNotIN(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWhereNOTIN";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 1,\n" +
                "\"fieldB\": \"abc\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": 3,\n" +
                "\"fieldB\": \"def\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"fieldA\": 5, \n" +
                "\"fieldB\": \"ghi\"}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * FROM \"%s\".\"%s\" WHERE \"fieldA\" NOT IN (1, 5)",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("102", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
            resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * FROM \"%s\".\"%s\" WHERE \"fieldB\" NOT IN ('abc', 'ghi')",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("102", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries containing casts from numbers work.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests queries with cast from number.")
    @ParameterizedTest(name = "testQueryCastNum - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryCastNum(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryCastNum";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 1,\n" +
                "\"fieldB\": 1.2," +
                "\"fieldC\": 10000000000}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT CAST(\"fieldA\" AS INTEGER), " +
                                    "CAST(\"fieldA\" AS BIGINT), " +
                                    "CAST(\"fieldA\" AS DOUBLE), " +
                                    "CAST(\"fieldB\" AS INTEGER)," +
                                    "CAST(\"fieldB\" AS BIGINT), " +
                                    "CAST(\"fieldB\" AS DOUBLE), " +
                                    "CAST(\"fieldC\" AS BIGINT), " +
                                    "CAST(\"fieldC\" AS DOUBLE) " +
                                    " FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(1, resultSet.getInt(1));
            Assertions.assertEquals(1L, resultSet.getLong(2));
            Assertions.assertEquals(1D, resultSet.getDouble(3));
            Assertions.assertEquals(1, resultSet.getInt(4));
            Assertions.assertEquals(1L, resultSet.getLong(5));
            Assertions.assertEquals(1.2D, resultSet.getDouble(6));
            Assertions.assertEquals(10000000000L, resultSet.getLong(7));
            Assertions.assertEquals(10000000000D, resultSet.getDouble(8));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries containing casts from strings work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Disabled("Cast to date not working.")
    @DisplayName("Tests queries with cast from string.")
    @ParameterizedTest(name = "testQueryCastString - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryCastString(final DocumentDbTestEnvironment testEnvironment) throws SQLException, ParseException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryCastString";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": \"2020-03-11\", \n" +
                "\"fieldNum\": \"100.5\"}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT CAST(\"fieldA\" AS DATE), " +
                                    "CAST(\"fieldA\" AS TIMESTAMP), " +
                                    "CAST(\"fieldNum\" AS DOUBLE), " +
                                    "CAST(\"fieldNum\" AS INTEGER)," +
                                    "CAST(\"fieldNum\" AS BIGINT) " +
                                    " FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("2020-03-11", resultSet.getString(1));
            Assertions.assertEquals(
                    new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime(),
                    resultSet.getTimestamp(2).getTime());
            Assertions.assertEquals(100.5D, resultSet.getDouble(3));
            Assertions.assertEquals(100, resultSet.getInt(4));
            Assertions.assertEquals(100, resultSet.getLong(5));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries containing casts from dates work.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests queries with cast from date.")
    @ParameterizedTest(name = "testQueryCastDate - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryCastDate(final DocumentDbTestEnvironment testEnvironment) throws SQLException, ParseException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryCastDate";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date",
                new BsonTimestamp(new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime()));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT CAST(\"date\" AS DATE), " +
                                    "CAST(\"date\" AS TIMESTAMP), " +
                                    "CAST(\"date\" AS TIME)," +
                                    "CAST(\"date\" AS VARCHAR) " +
                                    " FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("2020-03-11", resultSet.getDate(1).toString());
            Assertions.assertEquals(
                    new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime(),
                    resultSet.getTimestamp(2).getTime());
            Assertions.assertEquals(
                    new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime(),
                    resultSet.getTime(3).getTime());
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries containing casts from boolean work.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests queries with cast from boolean.")
    @ParameterizedTest(name = "testQueryCastBoolean - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryCastBoolean(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryCastBoolean";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": false}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT CAST(\"fieldA\" AS VARCHAR)" +
                                    " FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("false", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries containing various nested casts work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Disabled("Casts are not functioning from string to date.")
    @DisplayName("Tests queries with nested CAST.")
    @ParameterizedTest(name = "testQueryNestedCast - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryNestedCast(final DocumentDbTestEnvironment testEnvironment) throws SQLException, ParseException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryNestedCast";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"dateString\": \"2020-03-11\"," +
                "\"fieldNum\": 7," +
                "\"fieldString\": \"5\"}");
        doc1.append("fieldTimestamp", new BsonTimestamp(
                new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime()));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT CAST(CAST(\"dateString\" AS DATE) AS VARCHAR), " +
                                    "CAST(CAST(\"fieldTimestamp\" AS DATE) AS VARCHAR), " +
                                    "CAST(CAST(\"fieldNum\" AS DOUBLE) AS INTEGER)" +
                                    " FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("2020-03-11", resultSet.getString(1));
            Assertions.assertEquals("2020-03-11", resultSet.getString(2));
            Assertions.assertEquals(5, resultSet.getInt(3));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that the result set of a query does not close prematurely when results are retrieved in multiple batches.
     * Uses max fetch size of 10 to ensure multiple batches are retrieved.
     * @throws SQLException if connection or query fails.
     */
    @DisplayName("Tests that the result set does not close prematurely when results are retrieved in multiple batches.")
    @ParameterizedTest(name = "testResultSetDoesNotClose - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testResultSetDoesNotClose(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testResultsetClose";
        final int numDocs = 100;
        final BsonDocument[] docs = new BsonDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            final BsonDocument doc = BsonDocument.parse("{\"_id\": " + i + ", \n" +
                    "\"field\":\"abc\"}");
            docs[i] = doc;
        }
        insertBsonDocuments(tableName, docs);
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            statement.setFetchSize(1);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT * from \"%s\".\"%s\"", getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            for (int i = 0; i < numDocs; i++) {
                Assertions.assertTrue(resultSet.next());
                Assertions.assertEquals(String.valueOf(i), resultSet.getString(1));
            }
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that queries with a batch size of zero work.
     * @throws SQLException if connection or query fails.
     */
    @DisplayName("Tests that a batch size of zero works.")
    @ParameterizedTest(name = "testBatchSizeZero - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testBatchSizeZero(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testBatchSizeZero";
        final int numDocs = 10;
        final BsonDocument[] docs = new BsonDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            final BsonDocument doc = BsonDocument.parse("{\"_id\": " + i + ", \n" +
                    "\"field\":\"abc\"}");
            docs[i] = doc;
        }
        insertBsonDocuments(tableName, docs);
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            statement.setFetchSize(0);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT * from \"%s\".\"%s\"", getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            for (int i = 0; i < numDocs; i++) {
                Assertions.assertTrue(resultSet.next());
                Assertions.assertEquals(String.valueOf(i), resultSet.getString(1));
            }
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests for queries containing IS NULL and IS NOT NULL in the select clause.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests queries with IS [NOT] NULL in the select clause.")
    @ParameterizedTest(name = "testQuerySelectIsNull - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQuerySelectIsNull(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQuerySelectIsNull";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abc\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": null}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT \"field\" IS NULL, \"field\" IS NOT NULL FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);

            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.getBoolean(2));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertTrue(resultSet.getBoolean(1));
            Assertions.assertFalse(resultSet.getBoolean(2));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertTrue(resultSet.getBoolean(1));
            Assertions.assertFalse(resultSet.getBoolean(2));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Test that queries selecting a substring work.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Test that queries selecting a substring work.")
    @ParameterizedTest(name = "testQuerySubstring - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQuerySubstring(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testSelectQuerySubstring";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abcdefg\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"uvwxyz\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");
        final BsonDocument doc6 = BsonDocument.parse("{\"_id\": 106,\n" +
                "\"field\": \"ab\"}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5, doc6});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            // Test SUBSTRING(%1, %2, %3) format.
            final ResultSet resultSet1 = statement.executeQuery(
                    String.format("SELECT SUBSTRING(\"field\", 1, 3) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet1);
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("abc", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("uvw", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("ab", resultSet1.getString(1));
            Assertions.assertFalse(resultSet1.next());

            // Test SUBSTRING(%1, %2) format.
            final ResultSet resultSet2 = statement.executeQuery(
                    String.format("SELECT SUBSTRING(\"field\", 1) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet2);
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("abcdefg", resultSet2.getString(1));
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("uvwxyz", resultSet2.getString(1));
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("", resultSet2.getString(1));
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("", resultSet2.getString(1));
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("", resultSet2.getString(1));
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("ab", resultSet2.getString(1));
            Assertions.assertFalse(resultSet2.next());
        }
    }

    /**
     * Test that queries selecting a boolean expression with NOT work.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Test that queries selecting boolean expressions with NOT are correct.")
    @ParameterizedTest(name = "testQueryWithNot - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithNot(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWithNot";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field1\": true, \n" +
                "\"field2\": false, \n" +
                "\"field3\": 1}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field1\": true, \n" +
                "\"field2\": true, \n" +
                "\"field3\": 5}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field1\": false, \n" +
                "\"field2\": false, \n" +
                "\"field3\": 5}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT NOT (\"field1\"), " +
                                    "NOT (\"field1\" AND \"field2\"), " +
                                    "NOT (\"field1\" OR \"field2\"), " +
                                    "NOT (\"field1\" AND \"field3\" > 2) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.getBoolean(2));
            Assertions.assertFalse(resultSet.getBoolean(3));
            Assertions.assertTrue(resultSet.getBoolean(4));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertFalse(resultSet.getBoolean(2));
            Assertions.assertFalse(resultSet.getBoolean(3));
            Assertions.assertFalse(resultSet.getBoolean(4));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertTrue(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.getBoolean(2));
            Assertions.assertTrue(resultSet.getBoolean(3));
            Assertions.assertTrue(resultSet.getBoolean(4));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Test that queries selecting a boolean expression with NOT from nulls.
     * @throws SQLException occurs if query fails.
     */
    @Disabled("AD-315: Boolean expressions do not treat boolean operators with nulls correctly.")
    @DisplayName("Test that queries selecting a boolean expression with NOT from nulls are correct.")
    @ParameterizedTest(name = "testQueryWithAndOrNotNulls - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithAndOrNotNulls(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWithAndOrNotNulls";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101, \n" +
                "\"field1\": true, \n" + // Added this document only for metadata
                "\"field2\": true, \n" +
                "\"field3\": 1}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field1\": null, \n" +
                "\"field2\": null, \n" +
                "\"field3\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT NOT (\"field1\" AND \"field2\"), " +
                                    "NOT (\"field1\" OR \"field2\"), " +
                                    "NOT (\"field1\" AND \"field3\" > 2) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertNull(resultSet.getString(2));
            Assertions.assertNull(resultSet.getString(3));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Test that queries using COALESCE() are correct.")
    @ParameterizedTest(name = "testQueryCoalesce - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryCoalesce(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryCoalesce";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101, \n" +
                "\"field1\": null, \n" + // Added this document only for metadata
                "\"field2\": 1, \n" +
                "\"field3\": 2}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field1\": null, \n" +
                "\"field2\": null, \n" +
                "\"field3\": 2}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT COALESCE(\"%s\", \"%s\", \"%s\" ) FROM \"%s\".\"%s\"",
                            "field1", "field2", "field3", getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(resultSet.getInt(1), 1);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(resultSet.getInt(1), 2);
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Tests closing a Statement will not cause exception for cancelQuery.")
    @ParameterizedTest(name = "testCloseStatement - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCloseStatement(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            Assertions.assertDoesNotThrow(statement::close);
        }
    }

    @ParameterizedTest(name = "testQueryWithSelectBoolean - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQuerySelectBoolean(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQuerySelectBooleanExpr";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101, \n" +
                "\"field\": 1, \n " +
                "\"field2\": 2}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field\": 1, \n " +
                "\"field2\": 3}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103, \n" +
                "\"field\": 1, \n " +
                "\"field2\": null}");

        insertBsonDocuments(tableName,
                new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT \"field2\" <> 2 FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertTrue(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @ParameterizedTest(name = "testQueryWithNotNull - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQuerySelectNotWithNull(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQuerySelectNotNulls";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101, \n" +
                "\"field\": true}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field\": false}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103, \n" +
                "\"field\": null}");

        insertBsonDocuments(tableName,
                new BsonDocument[]{doc1, doc2, doc3});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT NOT(\"field\") FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertTrue(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @ParameterizedTest(name = "testQuerySelectLogicNulls - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQuerySelectLogicNulls(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQuerySelectLogicNulls";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101, \n" +
                "\"field\": true, \n" +
                "\"field1\": null}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field\": false, \n" +
                "\"field1\": null}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103, \n" +
                "\"field\": true, \n" +
                "\"field1\": true}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null, \n" +
                "\"field1\": null}");

        insertBsonDocuments(tableName,
                new BsonDocument[]{doc1, doc2, doc3, doc4});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT  " +
                                    "(\"field\" AND \"field1\")," +
                                    "(\"field\" OR \"field1\")" +
                                    "FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertTrue(resultSet.getBoolean(2));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertNull(resultSet.getString(2));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertTrue(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.getBoolean(2));


            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertNull(resultSet.getString(2));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @ParameterizedTest(name = "testQuerySelectLogicManyNulls - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQuerySelectLogicManyNulls(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQuerySelectLogicManyNulls";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101, \n" +
                "\"field\": true, \n" +
                "\"field1\": true, \n" +
                "\"field2\": null, \n" +
                "\"field3\": null, \n" +
                "\"field4\": true, \n" +
                "\"field5\": true, \n" +
                "\"field6\": false, \n" +
                "\"field7\": false, \n" +
                "\"field8\": true, \n" +
                "\"field9\": true}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field\": true, \n" +
                "\"field1\": true, \n" +
                "\"field2\": true, \n" +
                "\"field3\": true, \n" +
                "\"field4\": true, \n" +
                "\"field5\": null, \n" +
                "\"field6\": null, \n" +
                "\"field7\": true, \n" +
                "\"field8\": true, \n" +
                "\"field9\": null}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103, \n" +
                "\"field\": true, \n" +
                "\"field1\": true, \n" +
                "\"field2\": true, \n" +
                "\"field3\": true, \n" +
                "\"field4\": true, \n" +
                "\"field5\": true, \n" +
                "\"field6\": true, \n" +
                "\"field7\": true, \n" +
                "\"field8\": true, \n" +
                "\"field9\": true}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null, \n" +
                "\"field1\": null, \n" +
                "\"field2\": null, \n" +
                "\"field3\": null, \n" +
                "\"field4\": null, \n" +
                "\"field5\": null, \n" +
                "\"field6\": null, \n" +
                "\"field7\": null, \n" +
                "\"field8\": null, \n" +
                "\"field9\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105, \n" +
                "\"field\": null, \n" +
                "\"field1\": null, \n" +
                "\"field2\": null, \n" +
                "\"field3\": null, \n" +
                "\"field4\": null, \n" +
                "\"field5\": null, \n" +
                "\"field6\": false, \n" +
                "\"field7\": null, \n" +
                "\"field8\": null, \n" +
                "\"field9\": null}");

        insertBsonDocuments(tableName,
                new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT  " +
                                    "(\"field\" AND \"field1\" AND \"field2\" AND \"field3\" AND \"field4\" AND \"field5\" AND \"field6\" AND \"field7\" AND \"field8\" AND \"field9\")," +
                                    "(\"field\" OR \"field1\" OR \"field2\" OR \"field3\" OR \"field4\" OR \"field5\" OR \"field6\" OR \"field7\" OR \"field8\" OR \"field9\")" +
                                    "FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.getBoolean(2));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertTrue(resultSet.getBoolean(2));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertTrue(resultSet.getBoolean(1));
            Assertions.assertTrue(resultSet.getBoolean(2));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertNull(resultSet.getString(2));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.getBoolean(1));
            Assertions.assertNull(resultSet.getString(2));
            Assertions.assertFalse(resultSet.next());
        }
    }
}
