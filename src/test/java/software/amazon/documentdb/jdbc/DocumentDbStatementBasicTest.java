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

import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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
     * @param method the scan method to use
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     * @throws IOException  occurs if reading an input stream fails.
     */
    @ParameterizedTest
    @EnumSource(DocumentDbMetadataScanMethod.class)
    protected void testQueryWithAllDataTypes(final DocumentDbMetadataScanMethod method) throws SQLException, IOException {
        final String collectionName = "testDocumentDbDriverTest_" + method.getName();
        final int recordCount = 10;
        prepareSimpleConsistentData(DATABASE_NAME, collectionName,
                recordCount, USER, PASSWORD);
        final DocumentDbStatement statement = getDocumentDbStatement(method);
        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collectionName));
        Assertions.assertNotNull(resultSet);
        int count = 0;
        while (resultSet.next()) {
            Assertions.assertTrue(
                    Pattern.matches("^\\w+$", resultSet.getString(collectionName + "__id")));
            Assertions.assertEquals(Double.MAX_VALUE, resultSet.getDouble("fieldDouble"));
            Assertions.assertEquals("新年快乐", resultSet.getString("fieldString"));
            Assertions.assertTrue(Pattern.matches("^\\w+$", resultSet.getString("fieldObjectId")));
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
            resultSet.getBinaryStream("fieldBinary").read(actualBytes);
            Assertions.assertArrayEquals(expectedBytes, actualBytes);

            count++;
        }
        Assertions.assertEquals(recordCount, count);
    }

    /**
     * Test querying when there is a conflict between an array and a scalar. The column
     * should become VARCHAR.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testArrayScalarConflict() throws SQLException {
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
        insertBsonDocuments(
                "testArrayScalarConflict",
                DATABASE_NAME,
                USER,
                PASSWORD,
                documents.toArray(new BsonDocument[]{}));

        final DocumentDbStatement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, "testArrayScalarConflict_array"));
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

    /**
     * Test querying when there is a conflict between a document field and a scalar. The column
     * should become VARCHAR.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testDocumentScalarConflict() throws SQLException {
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
        insertBsonDocuments(
                "testDocumentScalarConflict",
                DATABASE_NAME,
                USER,
                PASSWORD,
                documents.toArray(new BsonDocument[]{}));

        final DocumentDbStatement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, "testDocumentScalarConflict"));
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

    /**
     * Test querying when there is a conflict between a document field and an array. The column
     * should become VARCHAR.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testArrayDocumentConflict() throws SQLException {
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
        insertBsonDocuments(
                "testArrayDocumentConflict",
                DATABASE_NAME,
                USER,
                PASSWORD,
                documents.toArray(new BsonDocument[]{}));

        final DocumentDbStatement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, "testArrayDocumentConflict"));
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

    /**
     * Test querying when there is a conflict between a document field and an array of mixed type. The column
     * should become VARCHAR.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testDocumentAndArrayOfMixedTypesConflict() throws SQLException {
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
        insertBsonDocuments(
                "testDocumentAndArrayOfMixedTypesConflict",
                DATABASE_NAME,
                USER,
                PASSWORD,
                documents.toArray(new BsonDocument[]{}));

        final DocumentDbStatement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, "testDocumentAndArrayOfMixedTypesConflict"));
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

    /**
     * Tests that documents missing a sub-document do not create null rows.
     */
    @DisplayName("Test that documents not containing a sub-document do not add null rows.")
    @Test
    void testDocumentWithMissingSubDocument() throws SQLException {
        final String collection = "testMissingSubdocumentNotNull";
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
        insertBsonDocuments(
                collection, DATABASE_NAME, USER, PASSWORD, documents.toArray(new BsonDocument[]{}));
        final Statement statement = getDocumentDbStatement();
        statement.execute(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection + "_subDocument"));
        final ResultSet results = statement.getResultSet();
        Assertions.assertTrue(results.next());
        Assertions.assertEquals("key0", results.getString(1));
        Assertions.assertEquals(1, results.getInt(2));
        Assertions.assertEquals(2, results.getInt(3));
        Assertions.assertFalse(results.next(), "Contained unexpected extra row.");
    }

    /**
     * Tests that documents missing a nested sub-document do not create null rows.
     */
    @DisplayName("Test that documents not containing a sub-document do not add null rows.")
    @Test
    void testDocumentWithMissingNestedSubDocument() throws SQLException {
        final String collection = "testMissingNestedSubdocumentNotNull";
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
        insertBsonDocuments(
                collection, DATABASE_NAME, USER, PASSWORD, documents.toArray(new BsonDocument[]{}));
        final Statement statement = getDocumentDbStatement();
        statement.execute(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, collection + "_subDocument_nestedSubDoc"));
        final ResultSet results = statement.getResultSet();
        Assertions.assertTrue(results.next());
        Assertions.assertEquals("key0", results.getString(1));
        Assertions.assertEquals(7, results.getInt(2));
        Assertions.assertFalse(results.next(), "Contained unexpected extra row.");
    }

    /**
     * Tests COUNT(columnName) doesn't count null or missing values.
     *
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests count('column') works ensuring null/undefined values are not counted")
    void testCountColumnName() throws SQLException {
        final String tableName = "testCountColumn";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abc\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": null}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103\n}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT COUNT(\"field\") from \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1, resultSet.getInt(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that SUM(1) works, equivalent to COUNT(*).
     *
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests query with SUM(1).")
    void testQuerySumOne() throws SQLException {
        final String tableName = "testQuerySumOne";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT SUM(1) from \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(3, resultSet.getInt(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that TIMESTAMPADD() works for intervals that can be converted to ms.
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests TIMESTAMPADD() with different intervals.")
    void testQueryTimestampAdd() throws SQLException {
        final String tableName = "testTimestampAdd";
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final long dayAfterDateTime = Instant.parse("2020-01-02T00:00:00.00Z").toEpochMilli();
        final long hourAfterDateTime =  Instant.parse("2020-01-01T01:00:00.00Z").toEpochMilli();
        final long minuteAfterDateTime = Instant.parse("2020-01-01T00:01:00.00Z").toEpochMilli();
        final long secondAfterDateTime =  Instant.parse("2020-01-01T00:00:01.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Add 1 day to a date column.
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(DAY"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(new Timestamp(dayAfterDateTime), resultSet.getTimestamp(1));
        Assertions.assertFalse(resultSet.next());

        // Add 1 hour to a date column.
        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(HOUR"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals(new Timestamp(hourAfterDateTime), resultSet2.getTimestamp(1));
        Assertions.assertFalse(resultSet2.next());

        // Add 1 minute to a date column.
        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(MINUTE"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertTrue(resultSet3.next());
        Assertions.assertEquals(new Timestamp(minuteAfterDateTime), resultSet3.getTimestamp(1));
        Assertions.assertFalse(resultSet3.next());

        // Add 1 second to a date column.
        final ResultSet resultSet4 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(SECOND"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet4);
        Assertions.assertTrue(resultSet4.next());
        Assertions.assertEquals(new Timestamp(secondAfterDateTime), resultSet4.getTimestamp(1));
        Assertions.assertFalse(resultSet4.next());

        // Add 1 day to a date literal.
        final ResultSet resultSet5 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(DAY"
                        + ", 1, TIMESTAMP '2020-01-01 00:00:00' ) FROM \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet5);
        Assertions.assertTrue(resultSet5.next());
        Assertions.assertEquals(new Timestamp(dayAfterDateTime), resultSet5.getTimestamp(1));
        Assertions.assertFalse(resultSet5.next());

        // Add 1 day to the date and extract the day of the month from result.
        final ResultSet resultSet6 = statement.executeQuery(
                String.format("SELECT DAYOFMONTH(TIMESTAMPADD(DAY"
                        + ", 1, \"field\")) FROM \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet6);
        Assertions.assertTrue(resultSet6.next());
        Assertions.assertEquals(2, resultSet6.getInt(1));
        Assertions.assertFalse(resultSet6.next());
    }

    /**
     * Tests that EXTRACT works for different time units.
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests EXTRACT() for different time units.")
    void testQueryExtract() throws SQLException {
        final String tableName = "testDateAdd";
        final long dateTime = Instant.parse("2020-02-03T04:05:06.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Get date parts.
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT "
                        + "YEAR(\"field\"), "
                        + "MONTH(\"field\"),"
                        + "WEEK(\"field\"),"
                        + "DAYOFMONTH(\"field\"),"
                        + "DAYOFWEEK(\"field\"),"
                        + "DAYOFYEAR(\"field\"),"
                        + "HOUR(\"field\"),"
                        + "MINUTE(\"field\"),"
                        + "SECOND(\"field\")"
                        + "FROM \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        // Year is 2020.
        Assertions.assertEquals(2020, resultSet.getInt(1));
        // Month is 2 (Feb).
        Assertions.assertEquals(2, resultSet.getInt(2));
        // Week in year is 5.
        Assertions.assertEquals(5, resultSet.getInt(3));
        // Day of month is 3.
        Assertions.assertEquals(3, resultSet.getInt(4));
        // Day of week is 2 (Mon).
        Assertions.assertEquals(2, resultSet.getInt(5));
        // Day of year is 34.
        Assertions.assertEquals(34, resultSet.getInt(6));
        // Hour is 4.
        Assertions.assertEquals(4, resultSet.getInt(7));
        // Minute is 5.
        Assertions.assertEquals(5, resultSet.getInt(8));
        // Seconds is 6.
        Assertions.assertEquals(6, resultSet.getInt(9));
        Assertions.assertFalse(resultSet.next());
    }


    /**
     * Tests that queries containing ORDER BY and OFFSET work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Test
    @DisplayName("Tests queries with OFFSET")
    void testQueryOffset() throws SQLException {
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
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" LIMIT 2 OFFSET 1",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries containing IN (c1, c2...) work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Test
    @DisplayName("Tests queries with WHERE [field] IN (...)")
    void testQueryWhereIN() throws SQLException {
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
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"fieldA\" IN (1, 5)",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
        resultSet = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"fieldB\" IN ('abc', 'ghi')",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries containing NOT IN (c1, c2...) work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Test
    @DisplayName("Tests queries with WHERE [field] NOT IN (...)")
    void testQueryWhereNotIN() throws SQLException {
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
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"fieldA\" NOT IN (1, 5)",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
        resultSet = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"fieldB\" NOT IN ('abc', 'ghi')",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries containing casts from numbers work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Test
    @DisplayName("Tests queries with cast from number.")
    void testQueryCastNum() throws SQLException {
        final String tableName = "testQueryCastNum";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 1,\n" +
                "\"fieldB\": 1.2," +
                "\"fieldC\": 10000000000}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();
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
                        DATABASE_NAME, tableName));
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

    /**
     * Tests that queries containing casts from strings work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Test
    @Disabled("Cast to date not working.")
    @DisplayName("Tests queries with cast from string.")
    void testQueryCastString() throws SQLException, ParseException {
        final String tableName = "testQueryCastString";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": \"2020-03-11\", \n" +
                "\"fieldNum\": \"100.5\"}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CAST(\"fieldA\" AS DATE), " +
                                "CAST(\"fieldA\" AS TIMESTAMP), " +
                                "CAST(\"fieldNum\" AS DOUBLE), " +
                                "CAST(\"fieldNum\" AS INTEGER)," +
                                "CAST(\"fieldNum\" AS BIGINT) " +
                                " FROM \"%s\".\"%s\"",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-03-11", resultSet.getString(1));
        Assertions.assertEquals(new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime(),
                resultSet.getTimestamp(2).getTime());
        Assertions.assertEquals(100.5D, resultSet.getDouble(3));
        Assertions.assertEquals(100, resultSet.getInt(4));
        Assertions.assertEquals(100, resultSet.getLong(5));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries containing casts from dates work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Test
    @Disabled("Cast to date not working.")
    @DisplayName("Tests queries with cast from date.")
    void testQueryCastDate() throws SQLException, ParseException {
        final String tableName = "testQueryCastDate";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date",
                new BsonTimestamp(new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime()));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CAST(\"date\" AS DATE), " +
                                "CAST(\"date\" AS TIMESTAMP), " +
                                "CAST(\"date\" AS TIME)," +
                                "CAST(\"date\" AS VARCHAR) " +
                                " FROM \"%s\".\"%s\"",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-03-11", resultSet.getDate(1).toString());
        Assertions.assertEquals(new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime(),
                resultSet.getTimestamp(2).getTime());
        Assertions.assertEquals(new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime(),
                resultSet.getTime(3).getTime());
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries containing casts from boolean work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Test
    @DisplayName("Tests queries with cast from boolean.")
    void testQueryCastBoolean() throws SQLException {
        final String tableName = "testQueryCastBoolean";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": false}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CAST(\"fieldA\" AS VARCHAR)" +
                                " FROM \"%s\".\"%s\"",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("false", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries containing various nested casts work.
     * @throws SQLException occurs if query or connection fails.
     */
    @Test
    @Disabled("Casts are not functioning from string to date.")
    @DisplayName("Tests queries with nested CAST.")
    void testQueryNestedCast() throws SQLException, ParseException {
        final String tableName = "testQueryNestedCast";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"dateString\": \"2020-03-11\"," +
                "\"fieldNum\": 7," +
                "\"fieldString\": \"5\"}");
        doc1.append("fieldTimestamp", new BsonTimestamp(
                new SimpleDateFormat("yyyy/MM/dd").parse("2020/03/11").getTime()));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CAST(CAST(\"dateString\" AS DATE) AS VARCHAR), " +
                                "CAST(CAST(\"fieldTimestamp\" AS DATE) AS VARCHAR), " +
                                "CAST(CAST(\"fieldNum\" AS DOUBLE) AS INTEGER)" +
                                " FROM \"%s\".\"%s\"",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("2020-03-11", resultSet.getString(1));
        Assertions.assertEquals("2020-03-11", resultSet.getString(2));
        Assertions.assertEquals(5, resultSet.getInt(3));
        Assertions.assertFalse(resultSet.next());
    }


    /**
     * Tests that the result set of a query does not close prematurely when results are retrieved in multiple batches.
     * Uses max fetch size of 10 to ensure multiple batches are retrieved.
     * @throws SQLException if connection or query fails.
     */
    @Test
    @DisplayName("Tests that the result set does not close prematurely when results are retrieved in multiple batches.")
    void testResultSetDoesNotClose() throws SQLException {
        final String tableName = "testResultsetClose";
        final int numDocs = 100;
        final BsonDocument[] docs = new BsonDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            final BsonDocument doc = BsonDocument.parse("{\"_id\": " + i + ", \n" +
                    "\"field\":\"abc\"}");
            docs[i] = doc;
        }
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD, docs);
        final Statement statement = getDocumentDbStatement();
        statement.setFetchSize(10);
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        for (int i = 0; i < numDocs; i++) {
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(String.valueOf(i), resultSet.getString(1));
        }
        Assertions.assertFalse(resultSet.next());
    }
}
