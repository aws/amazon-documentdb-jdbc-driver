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
import org.bson.BsonNull;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Pattern;

import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;

public class DocumentDbStatementBasicTest extends DocumentDbStatementTest {

    /**
     * Tests querying for all data types with all scan methods.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     * @throws IOException  occurs if reading an input stream fails.
     */
    @DisplayName("Tests that all supported data types can be scanned and retrieved.")
    @ParameterizedTest(name = "testQueryWithAllDataTypes - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    protected void testQueryWithAllDataTypes(final DocumentDbTestEnvironment testEnvironment) throws SQLException, IOException {
        setTestEnvironment(testEnvironment);
        final String collectionName = "testDocumentDbDriverTest";
        final int recordCount = 10;
        prepareSimpleConsistentData(collectionName, recordCount);
        final DocumentDbStatement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), collectionName));
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
            Assertions.assertEquals(3,
                    resultSet.getBinaryStream("fieldBinary").read(actualBytes));
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

        final DocumentDbStatement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), "testArrayScalarConflict_array"));
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
        final DocumentDbStatement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), "testDocumentScalarConflict"));
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
        final DocumentDbStatement statement = getDocumentDbStatement();
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
        final DocumentDbStatement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), "testDocumentAndArrayOfMixedTypesConflict"));
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
        final Statement statement = getDocumentDbStatement();
        statement.execute(String.format(
                "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), collection + "_subDocument"));
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
        final Statement statement = getDocumentDbStatement();
        statement.execute(String.format(
                "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), collection + "_subDocument_nestedSubDoc"));
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
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT COUNT(\"field\") from \"%s\".\"%s\"", getDatabaseName(), tableName));
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
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT SUM(1) from \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(3, resultSet.getInt(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests TIMESTAMPADD() and TIMESTAMPDIFF() for intervals that can be converted to ms.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests TIMESTAMPADD() and TIMESTAMPDIFF() with different intervals.")
    @ParameterizedTest(name = "testQueryTimestampAddDiff - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryTimestampAddDiff(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testTimestampAddDiff";
        final long dateTime = Instant.parse("2020-02-22T00:00:00.00Z").toEpochMilli();
        final long weekAfterDateTime = Instant.parse("2020-02-29T00:00:00.00Z").toEpochMilli();
        final long dayAfterDateTime = Instant.parse("2020-02-23T00:00:00.00Z").toEpochMilli();
        final long hourAfterDateTime =  Instant.parse("2020-02-22T01:00:00.00Z").toEpochMilli();
        final long minuteAfterDateTime = Instant.parse("2020-02-22T00:01:00.00Z").toEpochMilli();
        final long secondAfterDateTime =  Instant.parse("2020-02-22T00:00:01.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        doc1.append("fieldWeekAfter", new BsonDateTime(weekAfterDateTime));
        doc1.append("fieldDayAfter", new BsonDateTime(dayAfterDateTime));
        doc1.append("fieldHourAfter", new BsonDateTime(hourAfterDateTime));
        doc1.append("fieldMinuteAfter", new BsonDateTime(minuteAfterDateTime));
        doc1.append("fieldSecondAfter", new BsonDateTime(secondAfterDateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Add 1 day to a date column.
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(WEEK"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(new Timestamp(weekAfterDateTime), resultSet.getTimestamp(1));
        Assertions.assertFalse(resultSet.next());

        // Add 1 day to a date column.
        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(DAY"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(new Timestamp(dayAfterDateTime), resultSet1.getTimestamp(1));
        Assertions.assertFalse(resultSet1.next());

        // Add 1 hour to a date column.
        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(HOUR"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals(new Timestamp(hourAfterDateTime), resultSet2.getTimestamp(1));
        Assertions.assertFalse(resultSet2.next());

        // Add 1 minute to a date column.
        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(MINUTE"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertTrue(resultSet3.next());
        Assertions.assertEquals(new Timestamp(minuteAfterDateTime), resultSet3.getTimestamp(1));
        Assertions.assertFalse(resultSet3.next());

        // Add 1 second to a date column.
        final ResultSet resultSet4 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(SECOND"
                        + ", 1, \"field\") FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet4);
        Assertions.assertTrue(resultSet4.next());
        Assertions.assertEquals(new Timestamp(secondAfterDateTime), resultSet4.getTimestamp(1));
        Assertions.assertFalse(resultSet4.next());

        // Add 1 day to a date literal.
        final ResultSet resultSet5 = statement.executeQuery(
                String.format("SELECT TIMESTAMPADD(DAY"
                        + ", 1, TIMESTAMP '2020-02-22 00:00:00' ) FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet5);
        Assertions.assertTrue(resultSet5.next());
        Assertions.assertEquals(new Timestamp(dayAfterDateTime), resultSet5.getTimestamp(1));
        Assertions.assertFalse(resultSet5.next());

        // Add 1 day to the date and extract the day of the month from result.
        final ResultSet resultSet6 = statement.executeQuery(
                String.format("SELECT DAYOFMONTH(TIMESTAMPADD(DAY"
                        + ", 1, \"field\")) FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet6);
        Assertions.assertTrue(resultSet6.next());
        Assertions.assertEquals(23, resultSet6.getInt(1));
        Assertions.assertFalse(resultSet6.next());

        // Difference of DAY
        final ResultSet resultSet7 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(DAY, \"field\", \"fieldDayAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet7);
        Assertions.assertTrue(resultSet7.next());
        Assertions.assertEquals(1, resultSet7.getLong(1));
        Assertions.assertFalse(resultSet7.next());

        // Difference of WEEK
        final ResultSet resultSet8 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(WEEK, \"field\", \"fieldWeekAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet8);
        Assertions.assertTrue(resultSet8.next());
        Assertions.assertEquals(1, resultSet8.getLong(1));
        Assertions.assertFalse(resultSet8.next());

        // Difference of HOUR
        final ResultSet resultSet9 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(HOUR, \"field\", \"fieldHourAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet9);
        Assertions.assertTrue(resultSet9.next());
        Assertions.assertEquals(1, resultSet9.getLong(1));
        Assertions.assertFalse(resultSet9.next());

        // Difference of MINUTE
        final ResultSet resultSet10 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(MINUTE, \"field\", \"fieldMinuteAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet10);
        Assertions.assertTrue(resultSet10.next());
        Assertions.assertEquals(1, resultSet10.getLong(1));
        Assertions.assertFalse(resultSet10.next());

        // Difference of SECOND
        final ResultSet resultSet11 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(SECOND, \"field\", \"fieldSecondAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet11);
        Assertions.assertTrue(resultSet11.next());
        Assertions.assertEquals(1, resultSet11.getLong(1));
        Assertions.assertFalse(resultSet11.next());

        // Difference of MINUTE in SECOND
        final ResultSet resultSet12 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(SECOND, \"field\", \"fieldMinuteAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet12);
        Assertions.assertTrue(resultSet12.next());
        Assertions.assertEquals(60, resultSet12.getLong(1));
        Assertions.assertFalse(resultSet12.next());

        // Difference of HOUR in SECOND
        final ResultSet resultSet13 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(SECOND, \"field\", \"fieldHourAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet13);
        Assertions.assertTrue(resultSet13.next());
        Assertions.assertEquals(3600, resultSet13.getLong(1));
        Assertions.assertFalse(resultSet13.next());

        // Difference of DAY in SECOND
        final ResultSet resultSet14 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(SECOND, \"field\", \"fieldDayAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet14);
        Assertions.assertTrue(resultSet14.next());
        Assertions.assertEquals(86400, resultSet14.getLong(1));
        Assertions.assertFalse(resultSet14.next());

        // Difference of WEEK in SECOND
        final ResultSet resultSet15 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(SECOND, \"field\", \"fieldWeekAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet15);
        Assertions.assertTrue(resultSet15.next());
        Assertions.assertEquals(604800, resultSet15.getLong(1));
        Assertions.assertFalse(resultSet15.next());

        // Difference of SECOND in MICROSECOND
        final ResultSet resultSet16 = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(MICROSECOND, \"field\", \"fieldSecondAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet16);
        Assertions.assertTrue(resultSet16.next());
        Assertions.assertEquals(1000000, resultSet16.getLong(1));
        Assertions.assertFalse(resultSet16.next());
    }

    /**
     * Tests TIMESTAMPDIFF() for YEAR.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests TIMESTAMPDIFF() for YEAR.")
    @ParameterizedTest(name = "testQueryTimestampDiffYear - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryTimestampDiffYear(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testTimestampDiffYear";
        final long dateTime = Instant.parse("2020-02-22T00:00:00.00Z").toEpochMilli();
        final long yearAfterDateTime =  Instant.parse("2021-02-22T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        doc1.append("fieldYearAfter", new BsonDateTime(yearAfterDateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Difference of 12 months in YEAR
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(YEAR, \"field\", \"fieldYearAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1, resultSet.getLong(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests TIMESTAMPADD() for QUARTER.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests TIMESTAMPDIFF() for QUARTER.")
    @ParameterizedTest(name = "testQueryTimestampDiffQuarter - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryTimestampDiffQuarter(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testTimestampDiffQuarter";
        final long dateTime = Instant.parse("2020-02-22T00:00:00.00Z").toEpochMilli();
        final long yearAfterDateTime =  Instant.parse("2021-02-22T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        doc1.append("fieldYearAfter", new BsonDateTime(yearAfterDateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Difference of 12 months in QUARTER
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(QUARTER, \"field\", \"fieldYearAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(4, resultSet.getLong(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests TIMESTAMPADD() for MONTH.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests TIMESTAMPDIFF() for MONTH.")
    @ParameterizedTest(name = "testQueryTimestampDiffMonth - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryTimestampDiffMonth(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testTimestampDiffMonth";
        final long dateTime = Instant.parse("2020-02-22T00:00:00.00Z").toEpochMilli();
        final long yearAfterDateTime =  Instant.parse("2021-02-22T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        doc1.append("fieldYearAfter", new BsonDateTime(yearAfterDateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Difference of 12 months in MONTH
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT TIMESTAMPDIFF(MONTH, \"field\", \"fieldYearAfter\")"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(12, resultSet.getLong(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that EXTRACT works for different time units.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests EXTRACT() for different time units.")
    @ParameterizedTest(name = "testQueryExtract - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryExtract(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testExtract";
        final long dateTime = Instant.parse("2020-02-03T04:05:06.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();

        // Get date parts and use group by to remove any duplicate rows.
        final Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.SIMPLIFIED_CHINESE);
            // Get date parts.
            final ResultSet resultSet = statement.executeQuery(String.format(
                    "SELECT %n"
                            + "YEAR(\"field\"),%n"
                            + "MONTH(\"field\"),%n"
                            + "WEEK(\"field\"),%n"
                            + "DAYOFMONTH(\"field\"),%n"
                            + "DAYOFWEEK(\"field\"),%n"
                            + "DAYOFYEAR(\"field\"),%n"
                            + "HOUR(\"field\"),%n"
                            + "MINUTE(\"field\"),%n"
                            + "SECOND(\"field\"),%n"
                            + "QUARTER(\"field\"),%n"
                            + "DAYNAME(\"field\"),%n"
                            + "MONTHNAME(\"field\")%n"
                    + "FROM \"%s\".\"%s\" %n"
                    + "GROUP BY %n"
                            + "YEAR(\"field\"),%n"
                            + "MONTH(\"field\"),%n"
                            + "WEEK(\"field\"),%n"
                            + "DAYOFMONTH(\"field\"),%n"
                            + "DAYOFWEEK(\"field\"),%n"
                            + "DAYOFYEAR(\"field\"),%n"
                            + "HOUR(\"field\"),%n"
                            + "MINUTE(\"field\"),%n"
                            + "SECOND(\"field\"),%n"
                            + "QUARTER(\"field\"),%n"
                            + "DAYNAME(\"field\"),%n"
                            + "MONTHNAME(\"field\")",
                    getDatabaseName(), tableName));
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
            // Day of week is 2 (Monday).
            Assertions.assertEquals(2, resultSet.getInt(5));
            // Day of year is 34.
            Assertions.assertEquals(34, resultSet.getInt(6));
            // Hour is 4.
            Assertions.assertEquals(4, resultSet.getInt(7));
            // Minute is 5.
            Assertions.assertEquals(5, resultSet.getInt(8));
            // Seconds is 6.
            Assertions.assertEquals(6, resultSet.getInt(9));
            // Quarter is 1.
            Assertions.assertEquals(1, resultSet.getInt(10));
            // Day name is Monday
            Assertions.assertEquals("星期一", resultSet.getString(11));
            // Month name is February
            Assertions.assertEquals("二月", resultSet.getString(12));
            Assertions.assertFalse(resultSet.next());

        // Use extract in CASE.
        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT "
                        + "CASE WHEN DAYOFMONTH(\"field\") < 5 "
                        + "THEN 'A' "
                        + "ELSE 'B' "
                        + "END "
                        + "FROM \"%s\".\"%s\" ", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals("A", resultSet2.getString(1));
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals("A", resultSet2.getString(1));
        Assertions.assertFalse(resultSet2.next());
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    @DisplayName("Tests DAYNAME")
    @ParameterizedTest(name = "testDayName - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testDayName(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testDayName";
        final List<BsonDocument> docs = new ArrayList<>();
        final Instant startingDateTime = Instant.parse("2020-02-02T04:05:06.00Z");
        for (int i = 0; i < 8; i++) {
            docs.add(new BsonDocument(
                    "field",
                    new BsonDateTime(startingDateTime.plus(i, ChronoUnit.DAYS).toEpochMilli())));
        }
        insertBsonDocuments(tableName, docs.toArray(new BsonDocument[0]));
        final Statement statement = getDocumentDbStatement();

        final Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.SIMPLIFIED_CHINESE);
            // Get date parts.
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT "
                            + "DAYNAME(\"field\"), \"field\""
                            + "FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    new Timestamp(startingDateTime.plus(0, ChronoUnit.DAYS).toEpochMilli()),
                    resultSet.getTimestamp(2));
            Assertions.assertEquals(
                    DayOfWeek.SUNDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    DayOfWeek.MONDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    DayOfWeek.TUESDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    DayOfWeek.WEDNESDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    DayOfWeek.THURSDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    DayOfWeek.FRIDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    DayOfWeek.SATURDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    DayOfWeek.SUNDAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertFalse(resultSet.next());
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    @DisplayName("Tests MONTHNAME")
    @ParameterizedTest(name = "testMonthName - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testMonthName(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testMonthName";
        final List<BsonDocument> docs = new ArrayList<>();
        final OffsetDateTime startingDateTime = Instant.parse("2020-01-02T04:05:06.00Z").atOffset(ZoneOffset.UTC);
        for (int i = 0; i < 13; i++) {
            docs.add(new BsonDocument(
                    "field",
                    new BsonDateTime(startingDateTime.plusMonths(i).toInstant().toEpochMilli())));
        }
        docs.add(new BsonDocument(
                "field",
                new BsonNull()));
        insertBsonDocuments(tableName, docs.toArray(new BsonDocument[0]));
        final Statement statement = getDocumentDbStatement();

        final Locale originalLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.SIMPLIFIED_CHINESE);
            // Get date parts.
            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT "
                            + "MONTHNAME(\"field\"), \"field\""
                            + "FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    new Timestamp(startingDateTime.toInstant().toEpochMilli()),
                    resultSet.getTimestamp(2));
            Assertions.assertEquals(
                    Month.JANUARY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.FEBRUARY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.MARCH.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.APRIL.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.MAY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.JUNE.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.JULY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.AUGUST.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.SEPTEMBER.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.OCTOBER.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.NOVEMBER.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.DECEMBER.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(
                    Month.JANUARY.getDisplayName(TextStyle.FULL, Locale.getDefault()),
                    resultSet.getString(1));

            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));

            Assertions.assertFalse(resultSet.next());
        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    @DisplayName("Tests MONTHNAME for NULL")
    @ParameterizedTest(name = "testMonthNameForNull - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testMonthNameForNull(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testMonthNameForNull";
        final List<BsonDocument> docs = new ArrayList<>();
        docs.add(new BsonDocument(
                "field",
                new BsonNull()));
        insertBsonDocuments(tableName, docs.toArray(new BsonDocument[0]));
        final Statement statement = getDocumentDbStatement();

        // Get month name.
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT %n"
                        + " MONTHNAME(CAST(NULL AS TIMESTAMP))%n"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertNull(resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());

        // Get date parts.
        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT %n"
                        + " MONTHNAME(NULL)%n"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertNull(resultSet2.getString(1));
        Assertions.assertFalse(resultSet2.next());

        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT %n"
                        + " MONTHNAME(CAST(\"field\" AS TIMESTAMP))%n"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertTrue(resultSet3.next());
        Assertions.assertNull(resultSet3.getTimestamp(1));
        Assertions.assertFalse(resultSet3.next());
    }

    @DisplayName("Tests DAYNAME for NULL")
    @ParameterizedTest(name = "testDayNameForNull - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testDayNameForNull(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testDayNameForNull";
        final List<BsonDocument> docs = new ArrayList<>();
        docs.add(new BsonDocument(
                "field",
                new BsonNull()));
        insertBsonDocuments(tableName, docs.toArray(new BsonDocument[0]));
        final Statement statement = getDocumentDbStatement();

        // Get month name.
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT %n"
                        + " DAYNAME(CAST(NULL AS TIMESTAMP))%n"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertNull(resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());

        // Get date parts.
        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT %n"
                        + " DAYNAME(NULL)%n"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertNull(resultSet2.getString(1));
        Assertions.assertFalse(resultSet2.next());

        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT %n"
                        + " DAYNAME(CAST(\"field\" AS TIMESTAMP))%n"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertTrue(resultSet3.next());
        Assertions.assertNull(resultSet3.getTimestamp(1));
        Assertions.assertFalse(resultSet3.next());
    }

    /**
     * Tests CURRENT_TIMESTAMP.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests CURRENT_TIMESTAMP.")
    @ParameterizedTest(name = "testCurrentTimestamp - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCurrentTimestamp(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCurrentTimestamp";
        final long dateTime = Instant.parse("2020-02-03T04:05:06.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Get current date.
        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT CURRENT_TIMESTAMP"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));

        final Function<ResultSet, SQLException> validateResultSet = (testResultSet) -> {
            try {
                Assertions.assertNotNull(testResultSet);
                Assertions.assertTrue(testResultSet.next());
                Assertions.assertEquals(Types.TIMESTAMP, testResultSet.getMetaData().getColumnType(1));
                final Timestamp timestamp = testResultSet.getTimestamp(1);
                Assertions.assertNotNull(timestamp);
                final OffsetDateTime actualDateTime = timestamp.toInstant().atOffset(ZoneOffset.UTC);
                final OffsetDateTime currentDateTime = Instant.now().atOffset(ZoneOffset.UTC);
                final long diffInMilliSeconds = actualDateTime
                        .until(currentDateTime, ChronoUnit.MILLIS);
                Assertions.assertTrue(diffInMilliSeconds >= 0);
                Assertions.assertTrue(diffInMilliSeconds < 1000);
                Assertions.assertFalse(testResultSet.next());
                return null;
            } catch (SQLException e) {
                return e;
            }
        };
        SQLException e = validateResultSet.apply(resultSet1);
        if (e != null) {
            throw e;
        }

        // Get current date as alias column name.
        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT CURRENT_TIMESTAMP AS \"cts\""
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        e = validateResultSet.apply(resultSet2);
        if (e != null) {
            throw e;
        }

        // Where clause use.
        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT \"field\" "
                        + " FROM \"%s\".\"%s\""
                        + " WHERE \"field\" < CURRENT_TIMESTAMP", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertTrue(resultSet3.next());
        Assertions.assertEquals(dateTime, resultSet3.getTimestamp(1).getTime());
        Assertions.assertFalse(resultSet3.next());

        final ResultSet resultSet4 = statement.executeQuery(
                String.format("SELECT \"field\" "
                        + " FROM \"%s\".\"%s\""
                        + " WHERE \"field\" > CURRENT_TIMESTAMP", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet4);
        Assertions.assertFalse(resultSet4.next());
    }

    /**
     * Tests CURRENT_DATE.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests CURRENT_DATE.")
    @ParameterizedTest(name = "testCurrentDate - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCurrentDate(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCurrentDate";
        final long dateTime = Instant.parse("2020-02-03T04:05:06.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Get current date.
        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT CURRENT_DATE"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));

        final Function<ResultSet, SQLException> validateResultSet = (testResultSet) -> {
            try {
                Assertions.assertNotNull(testResultSet);
                Assertions.assertTrue(testResultSet.next());
                Assertions.assertEquals(Types.DATE, testResultSet.getMetaData().getColumnType(1));
                final Date actualDate = testResultSet.getDate(1);
                Assertions.assertNotNull(actualDate);
                final Date currentDate = new Date(Instant.now().toEpochMilli());
                Assertions.assertEquals(currentDate.toString(), actualDate.toString());
                Assertions.assertFalse(testResultSet.next());
                return null;
            } catch (SQLException e) {
                return e;
            }
        };
        SQLException e = validateResultSet.apply(resultSet1);
        if (e != null) {
            throw e;
        }

        // Get current date as alias column name.
        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT CURRENT_DATE AS \"cts\""
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        e = validateResultSet.apply(resultSet2);
        if (e != null) {
            throw e;
        }

        // Where clause use.
        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT \"field\" "
                        + " FROM \"%s\".\"%s\""
                        + " WHERE \"field\" < CURRENT_DATE", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertTrue(resultSet3.next());
        Assertions.assertEquals(dateTime, resultSet3.getDate(1).getTime());
        Assertions.assertFalse(resultSet3.next());

        final ResultSet resultSet4 = statement.executeQuery(
                String.format("SELECT \"field\" "
                        + " FROM \"%s\".\"%s\""
                        + " WHERE \"field\" > CURRENT_DATE", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet4);
        Assertions.assertFalse(resultSet4.next());
    }

    /**
     * Tests CURRENT_TIME.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests CURRENT_TIME.")
    @ParameterizedTest(name = "testCurrentTime - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCurrentTime(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCurrentTime";
        final long dateTime = Instant.parse("2020-02-03T04:05:06.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        // Get current date.
        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT CURRENT_TIME"
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));

        final Function<ResultSet, SQLException> validateResultSet = (testResultSet) -> {
            try {
                Assertions.assertNotNull(testResultSet);
                Assertions.assertTrue(testResultSet.next());
                Assertions.assertEquals(Types.TIME, testResultSet.getMetaData().getColumnType(1));
                final Time actualDate = testResultSet.getTime(1);
                Assertions.assertNotNull(actualDate);
                final Time currentTime = new Time(Instant.now().toEpochMilli());
                final long timeDiff =  actualDate.toLocalTime()
                        .until(currentTime.toLocalTime(), ChronoUnit.MILLIS);
                Assertions.assertTrue(timeDiff < 2000);
                Assertions.assertFalse(testResultSet.next());
                return null;
            } catch (SQLException e) {
                return e;
            }
        };
        SQLException e = validateResultSet.apply(resultSet1);
        if (e != null) {
            throw e;
        }

        // Get current date as alias column name.
        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT CURRENT_TIME AS \"cts\""
                        + " FROM \"%s\".\"%s\"", getDatabaseName(), tableName));
        e = validateResultSet.apply(resultSet2);
        if (e != null) {
            throw e;
        }

        // Where clause use - must cast to TIME to make it comparable.
        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT \"field\""
                        + " FROM \"%s\".\"%s\""
                        + " WHERE CAST(\"field\" AS TIME) < CURRENT_TIME", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertTrue(resultSet3.next());
        Assertions.assertEquals(dateTime, resultSet3.getDate(1).getTime());
        Assertions.assertFalse(resultSet3.next());

        final ResultSet resultSet4 = statement.executeQuery(
                String.format("SELECT \"field\""
                        + " FROM \"%s\".\"%s\""
                        + " WHERE CAST(\"field\" AS TIME) > CURRENT_TIME", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet4);
        Assertions.assertFalse(resultSet4.next());
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
        final Statement statement = getDocumentDbStatement();
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
        final Statement statement = getDocumentDbStatement();
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
        final Statement statement = getDocumentDbStatement();
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
        final Statement statement = getDocumentDbStatement();
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
        final Statement statement = getDocumentDbStatement();
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
    @DisplayName("Tests queries with cast from boolean.")
    @ParameterizedTest(name = "testQueryCastBoolean - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryCastBoolean(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryCastBoolean";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": false}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();
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
        final Statement statement = getDocumentDbStatement();
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
        final Statement statement = getDocumentDbStatement();
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
        final Statement statement = getDocumentDbStatement();
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
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT \"field\" IS NULL, \"field\" IS NOT NULL FROM \"%s\".\"%s\"",
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
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT SUBSTRING(\"field\", 1, 3) FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("abc", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("uvw", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("ab", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests for queries FLOOR(... TO ...) in select clause.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests for queries FLOOR(... TO ...) in select clause.")
    @ParameterizedTest(name = "testQuerySelectFloorForDate - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQuerySelectFloorForDate(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQuerySelectFloorForDate";
        final Instant dateTime = Instant.parse("2020-02-03T12:34:56.78Z");
        final OffsetDateTime offsetDateTime = dateTime.atOffset(ZoneOffset.UTC);
        final Instant epochDateTime = Instant.EPOCH;
        final OffsetDateTime offsetEpochDateTime = epochDateTime.atOffset(ZoneOffset.UTC);
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime.toEpochMilli()));
        doc1.append("fieldEpoch", new BsonDateTime(epochDateTime.toEpochMilli()));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT"
                                + " FLOOR(\"field\" TO YEAR),"
                                + " FLOOR(\"field\" TO MONTH),"
                                + " FLOOR(\"field\" TO QUARTER),"
                                + " FLOOR(\"field\" TO DAY),"
                                + " FLOOR(\"field\" TO HOUR),"
                                + " FLOOR(\"field\" TO MINUTE),"
                                + " FLOOR(\"field\" TO SECOND),"
                                + " FLOOR(\"field\" TO MILLISECOND),"
                                + " FLOOR(\"fieldEpoch\" TO YEAR),"
                                + " FLOOR(\"fieldEpoch\" TO MONTH),"
                                + " FLOOR(\"fieldEpoch\" TO QUARTER),"
                                + " FLOOR(\"fieldEpoch\" TO DAY),"
                                + " FLOOR(\"fieldEpoch\" TO HOUR),"
                                + " FLOOR(\"fieldEpoch\" TO MINUTE),"
                                + " FLOOR(\"fieldEpoch\" TO SECOND),"
                                + " FLOOR(\"fieldEpoch\" TO MILLISECOND),"
                                + " FLOOR(NULL TO MILLISECOND)"
                                + " FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetDateTime, 1),
                resultSet.getTimestamp(1).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetDateTime, offsetDateTime.getMonthValue()),
                resultSet.getTimestamp(2).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetDateTime, 1),
                resultSet.getTimestamp(3).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.DAYS),
                resultSet.getTimestamp(4).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.HOURS),
                resultSet.getTimestamp(5).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.MINUTES),
                resultSet.getTimestamp(6).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.SECONDS),
                resultSet.getTimestamp(7).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.MILLIS),
                resultSet.getTimestamp(8).getTime());

        Assertions.assertEquals(
                getTruncatedTimestamp(offsetEpochDateTime, 1),
                resultSet.getTimestamp(9).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetEpochDateTime, offsetEpochDateTime.getMonthValue()),
                resultSet.getTimestamp(10).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetEpochDateTime, 1),
                resultSet.getTimestamp(11).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(epochDateTime, ChronoUnit.DAYS),
                resultSet.getTimestamp(12).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(epochDateTime, ChronoUnit.HOURS),
                resultSet.getTimestamp(13).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(epochDateTime, ChronoUnit.MINUTES),
                resultSet.getTimestamp(14).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(epochDateTime, ChronoUnit.SECONDS),
                resultSet.getTimestamp(15).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(epochDateTime, ChronoUnit.MILLIS),
                resultSet.getTimestamp(16).getTime());
        Assertions.assertNull(resultSet.getTimestamp(17));
        Assertions.assertFalse(resultSet.next());

        // Test WEEK (to Monday) truncation
        final ResultSet resultSet1 = statement.executeQuery(String.format(
                "SELECT"
                        + " FLOOR(\"field\" TO WEEK)," // Monday
                        + " FLOOR(TIMESTAMPADD(DAY, 1, \"field\") TO WEEK)," // Tuesday
                        + " FLOOR(TIMESTAMPADD(DAY, 2, \"field\") TO WEEK)," // Wednesday
                        + " FLOOR(TIMESTAMPADD(DAY, 3, \"field\") TO WEEK)," // Thursday
                        + " FLOOR(TIMESTAMPADD(DAY, 4, \"field\") TO WEEK)," // Friday
                        + " FLOOR(TIMESTAMPADD(DAY, 5, \"field\") TO WEEK)," // Saturday
                        + " FLOOR(TIMESTAMPADD(DAY, 6, \"field\") TO WEEK)," // Sunday
                        + " FLOOR(TIMESTAMPADD(DAY, 7, \"field\") TO WEEK)," // Next week
                        + " FLOOR(NULL TO WEEK)" // NULL
                        + " FROM \"%s\".\"%s\"",
                getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.DAYS),
                resultSet1.getTimestamp(1).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.DAYS),
                resultSet1.getTimestamp(2).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.DAYS),
                resultSet1.getTimestamp(3).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.DAYS),
                resultSet1.getTimestamp(4).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.DAYS),
                resultSet1.getTimestamp(5).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.DAYS),
                resultSet1.getTimestamp(6).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(dateTime, ChronoUnit.DAYS),
                resultSet1.getTimestamp(7).getTime());
        // Next week
        Assertions.assertEquals(
                getTruncatedTimestamp(
                        dateTime
                                .atOffset(ZoneOffset.UTC)
                                .plus(1, ChronoUnit.WEEKS)
                                .toInstant(), ChronoUnit.DAYS),
                resultSet1.getTimestamp(8).getTime());
        Assertions.assertNull(resultSet1.getTimestamp(9));
        Assertions.assertFalse(resultSet1.next());

        // Test QUARTER truncation
        final ResultSet resultSet2 = statement.executeQuery(String.format(
                "SELECT %n"
                        + " FLOOR(\"field\" TO QUARTER), %n"
                        + " FLOOR(TIMESTAMPADD(DAY, 100, \"field\") TO QUARTER), %n"
                        + " FLOOR(TIMESTAMPADD(DAY, 200, \"field\") TO QUARTER), %n"
                        + " FLOOR(TIMESTAMPADD(DAY, 300, \"field\") TO QUARTER), %n"
                        + " FLOOR(NULL TO QUARTER) %n"
                        + " FROM \"%s\".\"%s\"",
                getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetDateTime, 1), // Jan. 01
                resultSet2.getTimestamp(1).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetDateTime, 4), // Apr. 01
                resultSet2.getTimestamp(2).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetDateTime, 7), // Jul. 01
                resultSet2.getTimestamp(3).getTime());
        Assertions.assertEquals(
                getTruncatedTimestamp(offsetDateTime, 10), // Oct. 01
                resultSet2.getTimestamp(4).getTime());
        Assertions.assertNull(resultSet2.getTimestamp(5)); // NULL
        Assertions.assertFalse(resultSet2.next());

        // Don't support FLOOR for numeric, yet.
        final String errorMessage = Assertions.assertThrows(
                SQLException.class,
                () -> statement.executeQuery(
                        String.format("SELECT FLOOR(12.34) FROM \"%s\".\"%s\"",
                                getDatabaseName(), tableName))).getMessage();
        Assertions.assertTrue(
                errorMessage.startsWith(String.format(
                        "Unable to parse SQL 'SELECT FLOOR(12.34) FROM \"%s\".\"testQuerySelectFloorForDate\"'.", getDatabaseName()))
                && errorMessage.endsWith(
                        "Additional info: 'Translation of FLOOR(12.34:DECIMAL(4, 2)) is not supported by DocumentDbRules''"));
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
        final Statement statement = getDocumentDbStatement();
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

    /**
     * Test that queries selecting a boolean expression with NOT from nulls.
     * @throws SQLException occurs if query fails.
     */
    @Disabled("AD-267: Boolean expressions do not treat nulls correctly.")
    @DisplayName("Test that queries selecting a boolean expression with NOT from nulls are correct.")
    @ParameterizedTest(name = "testQueryWithNotNulls - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithNotNulls(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWithNotNulls";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101, \n" +
                "\"field1\": true, \n" + // Added this document only for metadata
                "\"field2\": true, \n" +
                "\"field3\": 1}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field1\": null, \n" +
                "\"field2\": null, \n" +
                "\"field3\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT NOT (\"field1\"), " +
                                "NOT (\"field1\" AND \"field2\"), " +
                                "NOT (\"field1\" OR \"field2\"), " +
                                "NOT (\"field1\" AND \"field3\" > 2) FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.next());
        Assertions.assertNull(resultSet.getString(1));
        Assertions.assertNull(resultSet.getString(2));
        Assertions.assertNull(resultSet.getString(3));
        Assertions.assertNull(resultSet.getString(4));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests closing a Statement will not cause exception for cancelQuery.")
    @ParameterizedTest(name = "testCloseStatement - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCloseStatement(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final Statement statement = getDocumentDbStatement();
        Assertions.assertDoesNotThrow(statement::close);
    }

    private long getTruncatedTimestamp(final OffsetDateTime offsetDateTime, final int monthValue) {
        return OffsetDateTime.of(
                offsetDateTime.getYear(), monthValue, 1,
                0, 0, 0, 0,
                ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
    }

    private long getTruncatedTimestamp(final Instant dateTime, final ChronoUnit chronoUnit) {
        return dateTime
                .atOffset(ZoneOffset.UTC)
                .truncatedTo(chronoUnit)
                .toInstant()
                .toEpochMilli();
    }
}
