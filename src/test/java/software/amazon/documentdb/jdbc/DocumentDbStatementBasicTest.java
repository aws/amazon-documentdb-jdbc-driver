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
import org.junit.jupiter.api.Assertions;
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
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Get date parts and use group by to remove duplicates.
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
                        + "FROM \"%s\".\"%s\" "
                        + "GROUP BY "
                        + "YEAR(\"field\"), "
                        + "MONTH(\"field\"),"
                        + "WEEK(\"field\"),"
                        + "DAYOFMONTH(\"field\"),"
                        + "DAYOFWEEK(\"field\"),"
                        + "DAYOFYEAR(\"field\"),"
                        + "HOUR(\"field\"),"
                        + "MINUTE(\"field\"),"
                        + "SECOND(\"field\")", DATABASE_NAME, tableName));
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
}
