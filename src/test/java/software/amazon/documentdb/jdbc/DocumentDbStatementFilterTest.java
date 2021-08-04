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

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonMinKey;
import org.bson.BsonString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;

public class DocumentDbStatementFilterTest extends DocumentDbStatementTest {

    /**
     * Tests that a statement with project, where, group by, having, order, and limit works for a single table.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit works for a single table.")
    @ParameterizedTest(name = "testComplexQuery - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testComplexQuery(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String collection = "testComplexQuery";
        final BsonDocument document1 =
                BsonDocument.parse("{ \"_id\" : \"key0\", \"array\": [1, 2, 3, 4, 5] }");
        final BsonDocument document2 =
                BsonDocument.parse("{ \"_id\" : \"key1\", \"array\": [1, 2, 3] }");
        final BsonDocument document3 =
                BsonDocument.parse("{ \"_id\" : \"key2\", \"array\": [1, 2] }");
        final BsonDocument document4 =
                BsonDocument.parse("{ \"_id\" : \"key3\", \"array\": [1, 2, 3, 4, 5] }");
        insertBsonDocuments(collection, new BsonDocument[]{document1, document2, document3, document4});
        final Statement statement = getDocumentDbStatement();

        // Verify that result set has correct values.
        statement.execute(String.format(
                "SELECT \"%s\", COUNT(*) AS \"Count\" FROM \"%s\".\"%s\""
                        + "WHERE \"%s\" <> 'key3' "
                        + "GROUP BY \"%s\" HAVING COUNT(*) > 1"
                        + "ORDER BY \"Count\" DESC LIMIT 1",
                collection + "__id",
                getDatabaseName(), collection + "_array",
                collection + "__id",
                collection + "__id"));
        final ResultSet resultSet1 = statement.getResultSet();
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals("key0", resultSet1.getString(collection + "__id"));
        Assertions.assertEquals(5, resultSet1.getInt("Count"));
        Assertions.assertFalse(resultSet1.next());
    }

    /**
     * Tests that queries with not-equals do not return null or undefined values.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests that comparisons to null do not return a value.")
    @ParameterizedTest(name = "testComparisonToNull - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testComparisonToNull(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testComparisonsToNull";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n}" +
                "\"field\": null");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"field\" <> 5", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests query with two literals in a where clause such that the comparison is true.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests query WHERE clause containing two literals such that the comparison is true.")
    @ParameterizedTest(name = "testQueryWhereTwoLiteralsTrue - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWhereTwoLiteralsTrue(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWhereTwoLiteralsTrue";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE 2 > 1", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(4, resultSet.getInt(2));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertNull(resultSet.getString(2));
        Assertions.assertFalse(resultSet.next());
    }

    /** Tests that queries with multiple not-equals clauses are correct.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests that multiple != conditions can be used.")
    @ParameterizedTest(name = "testMultipleNotEquals - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testMultipleNotEquals(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testMultipleNotEquals";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field\": 3}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103, \n" +
                "\"field\": 2}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"field\" <> 4 AND \"field\" <> 3", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(2, resultSet.getInt(2));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests query with boolean literal values.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests query WHERE clause with boolean literal.")
    @ParameterizedTest(name = "testQueryWhereLiteralBoolean - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWhereLiteralBoolean(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWhereLiteralBoolean";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": true, \n " +
                "\"fieldB\": false}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": false, \n " +
                "\"fieldB\": true}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"fieldA\" = TRUE AND \"fieldB\" = FALSE", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(2));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries with CASE are correct, particularly where null or undefined values are involved.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests queries with CASE and null values are correct.")
    @ParameterizedTest(name = "testCase - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testCase(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCASE";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n"
                + "\"field\": 1}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n"
                + "\"field\": 2}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n"
                + "\"field\": 5}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104,\n"
                + "\"field\": 4}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105,\n"
                + "\"field\": 3}");
        final BsonDocument doc6 = BsonDocument.parse("{\"_id\": 106,\n"
                + "\"field2\": 9}");
        final BsonDocument doc7 = BsonDocument.parse("{\"_id\": 107,\n"
                + "\"field\": null}");
        final BsonDocument doc8 = BsonDocument.parse("{\"_id\": 108}");
        final BsonDocument doc9 = BsonDocument.parse("{\"_id\": 109}");
        doc9.append("field", new BsonMinKey());
        insertBsonDocuments(tableName,
                new BsonDocument[]{doc1, doc2, doc3, doc4, doc5, doc6, doc7, doc8, doc9});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CASE "
                                + "WHEN \"field\" < 2  THEN 'A' "
                                + "WHEN \"field\" <= 2 THEN 'B' "
                                + "WHEN \"field\" > 4 THEN 'C' "
                                + "WHEN \"field\" >= 4 THEN 'D' "
                                + "WHEN \"field\" <> 7 THEN 'E' "
                                + "WHEN \"field2\" IN (9, 10) THEN 'F' "
                                + "ELSE 'G' END FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("A", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("B", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("C", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("D", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("E", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("F", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("G", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("G", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("G", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests query with a where clause comparing two fields.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests query with WHERE clause comparing two fields.")
    @ParameterizedTest(name = "testQueryWhereTwoColumns - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWhereTwoColumns(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWhereTwoFields";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 4, \n " +
                "\"fieldB\": 5}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": 5, \n " +
                "\"fieldB\": 4}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"fieldA\" < \"fieldB\"", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(4, resultSet.getInt(2));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries with CASE are correct with two different fields involved.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests queries with two field CASE.")
    @ParameterizedTest(name = "testCaseTwoFields - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testCaseTwoFields(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCASETwoFields";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n"
                + "\"fieldA\": 1,\n"
                + "\"fieldB\": 2}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n"
                + "\"fieldA\": 2,\n"
                + "\"fieldB\": 1}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n"
                + "\"fieldA\": 1}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CASE "
                                + "WHEN \"fieldA\" < \"fieldB\"  THEN 'A' "
                                + "WHEN \"fieldA\" > \"fieldB\" THEN 'B' "
                                + "ELSE 'C' END FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("A", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("B", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("C", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries can contain nested OR conditions in WHERE clause.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests queries with nested OR.")
    @ParameterizedTest(name = "testWhereNestedOR - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWhereNestedOR(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testNestedOR";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 1,\n" +
                "\"fieldB\": 2}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": 2,\n" +
                "\"fieldB\": 1}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"fieldA\": 1}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104,\n" +
                "\"fieldA\": 13, \n" +
                "\"fieldB\": 1}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105,\n" +
                "\"fieldA\": 1, \n" +
                "\"fieldB\": 10}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" " +
                        "WHERE (\"fieldA\" < 3  OR \"fieldB\" < 2) " +
                        "AND (\"fieldA\" > 12 OR \"fieldB\" > 8)", getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(104, resultSet.getInt(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(105, resultSet.getInt(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries with various types in WHERE clause")
    @ParameterizedTest(name = "testQueryWhereTypes - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWhereTypes(final DocumentDbTestEnvironment testEnvironment)throws  SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWhereTypes";
        final BsonDateTime date = new BsonDateTime(Instant.now().toEpochMilli());
        final long bigInt = 100000000000L;
        final double doubleValue = 1.2345;
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": \"abc\", \n " +
                "\"fieldB\": 5}");
        doc1.append("fieldC", BsonBoolean.TRUE);
        doc1.append("fieldD", date);
        doc1.append("fieldE", new BsonInt64(bigInt));
        doc1.append("fieldF", new BsonDouble(doubleValue));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": \"def\", \n " +
                "\"fieldB\": 4}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"fieldA\" = 'abc' AND " +
                        "\"fieldB\" = 5 AND \"fieldC\" = TRUE AND \"fieldD\" > '2020-03-11' AND \"fieldE\" = %d AND \"fieldF\" = %f",
                        getDatabaseName(), tableName, bigInt, doubleValue));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("abc", resultSet.getString(2));
        Assertions.assertEquals(5, resultSet.getInt(3));
        Assertions.assertTrue(resultSet.getBoolean(4));
        Assertions.assertEquals(date.getValue(), resultSet.getTimestamp(5).getTime());
        Assertions.assertEquals(bigInt, resultSet.getLong(6));
        Assertions.assertEquals(doubleValue, resultSet.getDouble(7));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that date literals can be used in WHERE comparisons.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests that date literals can be used in WHERE comparisons")
    @ParameterizedTest(name = "testQueryWhereDateLiteral - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWhereDateLiteral(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testDateLiteral";
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" < DATE '2020-01-02'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet1.getTimestamp(2));
        Assertions.assertFalse(resultSet1.next());

        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = DATE '2020-01-01'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet2.getTimestamp(2));
        Assertions.assertFalse(resultSet2.next());

        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" <> DATE '2020-01-01'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertFalse(resultSet3.next());
    }

    /**
     * Tests that date literals can be used in WHERE comparisons.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests that timestamp literals can be used in WHERE comparisons")
    @ParameterizedTest(name = "testQueryWhereTimestampLiteral - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWhereTimestampLiteral(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testTimestampLiteral";
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" < TIMESTAMP '2020-01-02 00:00:00'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet1.getTimestamp(2));
        Assertions.assertFalse(resultSet1.next());

        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = TIMESTAMP '2020-01-01 00:00:00'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet2.getTimestamp(2));
        Assertions.assertFalse(resultSet2.next());

        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" <> TIMESTAMP '2020-01-01 00:00:00'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertFalse(resultSet3.next());
    }

    /**
     * Tests that calls to timestampAdd can be used in WHERE comparisons.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests that timestampadd can be used in WHERE comparisons")
    @ParameterizedTest(name = "testQueryWhereTimestampAdd - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWhereTimestampAdd(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereTimestampAdd";
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE TIMESTAMPADD(DAY, 1, \"field\") = TIMESTAMP '2020-01-02 00:00:00'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet1.getTimestamp(2));
        Assertions.assertFalse(resultSet1.next());
    }

    /**
     * Tests for queries filtering by IS NULL.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests for IS NULL")
    @ParameterizedTest(name = "testQueryWithIsNull - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWithIsNull(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereQueryIsNull";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abc\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": null}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" IS NULL",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests for queries filtering by IS NOT NULL.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests for IS NOT NULL")
    @ParameterizedTest(name = "testQueryWithIsNotNull - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWithIsNotNull(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereQueryIsNotNull";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abc\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": null}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" IS NOT NULL",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests for CASE statements containing IS NULL and IS NOT NULL.
     *
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests for CASE statements with IS [NOT] NULL")
    @ParameterizedTest(name = "testQueryWithIsNotNullCase - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryWithIsNotNullCase(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereQueryIsNotNullCase";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abc\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": null}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT CASE " +
                                "WHEN \"field\" IS NULL THEN 1" +
                                "WHEN \"field\" IS NOT NULL THEN 2" +
                                "ELSE 3 END " +
                                "FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(2, resultSet.getInt(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1, resultSet.getInt(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(1, resultSet.getInt(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests a query with CASE in the WHERE clause.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests query with WHERE and CASE.")
    @ParameterizedTest(name = "testWhereWithCase - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWhereWithCase(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereCASE";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n"
                + "\"field\": 1}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n"
                + "\"field\": 2}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n"
                + "\"field\": 5}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104,\n"
                + "\"field\": 4}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105,\n"
                + "\"field\": 3}");
        final BsonDocument doc6 = BsonDocument.parse("{\"_id\": 106,\n"
                + "\"field\": null}");
        final BsonDocument doc7 = BsonDocument.parse("{\"_id\": 107}");
        final BsonDocument doc8 = BsonDocument.parse("{\"_id\": 108}");
        doc8.append("field", new BsonMinKey());
        insertBsonDocuments(tableName,
                new BsonDocument[]{doc1, doc2, doc3, doc4, doc5, doc6, doc7, doc8});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" "
                                + "WHERE (CASE "
                                + "WHEN \"field\" < 2  THEN 'A' "
                                + "WHEN \"field\" <= 2 THEN 'B' "
                                + "WHEN \"field\" > 4 THEN 'C' "
                                + "WHEN \"field\" >= 4 THEN 'D' "
                                + "WHEN \"field\" <> 7 THEN 'E' "
                                + "ELSE 'F' END) = 'A'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(101, resultSet.getInt(1));
        Assertions.assertEquals(1, resultSet.getInt(2));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests queries with WHERE using string literals with '$'.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests queries with WHERE using string literals with '$'.")
    @ParameterizedTest(name = "testWhereWithConflictingStringLiterals - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWhereWithConflictingStringLiterals(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereWithConflictingStringLiterals";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n"
                + "\"price\": \"$1\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n"
                + "\"price\": \"$2.25\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n"
                + "\"price\": \"1\"}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet1 = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" "
                                + "WHERE \"price\" = '$1'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(101, resultSet1.getInt(1));
        Assertions.assertEquals("$1", resultSet1.getString(2));
        Assertions.assertFalse(resultSet1.next());
    }

    /**
     * Tests a query with nested CASE.
     */
    @DisplayName("Tests a query with nested CASE.")
    @ParameterizedTest(name = "testNestedCase - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testNestedCase(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testNestedCASE";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n"
                + "\"field\": 1}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n"
                + "\"field\": 2}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n"
                + "\"field\": 3}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CASE "
                                + "WHEN \"field\" < 3  THEN "
                                + "( CASE WHEN \"field\" < 2 THEN 'A' "
                                + "ELSE 'B' END )"
                                + "ELSE 'C' END FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("A", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("B", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("C", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests queries with CASE where a string literal contains '$'.
     */
    @DisplayName("Tests queries with CASE where a string literal contains '$'.")
    @ParameterizedTest(name = "testCaseWithConflictingStringLiterals - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testCaseWithConflictingStringLiterals(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCaseWithConflictingStringLiterals";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n"
                + "\"price\": \"$1\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n"
                + "\"price\": \"$2.25\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n"
                + "\"price\": \"1\"}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet1 = statement.executeQuery(
                String.format(
                        "SELECT CASE "
                                + "WHEN \"price\" = '$1'  THEN 'A' "
                                + "ELSE 'B' END FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals("A", resultSet1.getString(1));
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals("B", resultSet1.getString(1));
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals("B", resultSet1.getString(1));
        Assertions.assertFalse(resultSet1.next());

        final ResultSet resultSet2 = statement.executeQuery(
                String.format(
                        "SELECT CASE "
                                + "WHEN \"price\" = '1'  THEN 'YES' "
                                + "ELSE '$price' END FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals("$price", resultSet2.getString(1));
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals("$price", resultSet2.getString(1));
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals("YES", resultSet2.getString(1));
        Assertions.assertFalse(resultSet2.next());
    }

    @DisplayName("Tests queries with CASE with boolean columns.")
    @ParameterizedTest(name = "testCaseWithBooleanColumns - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testCaseWithBooleanColumns(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCaseWithBooleanColumns";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n"
                + "\"field\": true }");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n"
                + "\"field\": false }");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103 }");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CASE "
                                + "WHEN \"field\" THEN 'Yes' "
                                + "WHEN NOT \"field\" THEN 'No' "
                                + "ELSE 'Unknown' END FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("Yes", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("No", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("Unknown", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries with substring work.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Test that queries filtering with substring work.")
    @ParameterizedTest(name = "testQuerySubstring - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQuerySubstring(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereQuerySubstring";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abcdefg\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"uvwxyz\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE SUBSTRING(\"field\", 2, 3) = 'bcd'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries with substring without a length input work.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Test that queries filtering with substring without a length input work.")
    @ParameterizedTest(name = "testQuerySubstringNoLength - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQuerySubstringNoLength(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereQuerySubstringNoLength";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abcdefg\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"abcdefgh\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE SUBSTRING(\"field\", 2) = 'bcdefg'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that queries with case containing substring.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Test that queries with case containing substring work.")
    @ParameterizedTest(name = "testQueryCaseSubstring - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testQueryCaseSubstring(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCaseQuerySubstring";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abcdefg\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"abcmno\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT CASE " +
                                "WHEN SUBSTRING(\"field\", 1, 4) = 'abcd' THEN 'A'" +
                                "WHEN SUBSTRING(\"field\", 1, 3) = 'abc' THEN 'B'" +
                                "ELSE 'C' END FROM \"%s\".\"%s\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("A", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("B", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("C", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("C", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests that substring works with a literal.
     * @throws SQLException occurs if query fails.
     */
    @DisplayName("Tests substring with a literal.")
    @ParameterizedTest(name = "testSubstringLiteral - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testSubstringLiteral(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testSubstringLiteral";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abc\", \n" +
                "\"field2\": 3}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"abcmno\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = SUBSTRING('abcdef', 1, 3)",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests substring with expressions for index and length.")
    @ParameterizedTest(name = "testComplexQuery - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testSubstringExpressions(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testSubstringExpressions";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abcdef\", \n" +
                "\"field2\": 3, \n" +
                "\"field3\": 1}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"abcdef\", \n" +
                "\"field2\": 2 \n" +
                "\"field3\": 1}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\", \n" +
                "\"field2\": 3, \n" +
                "\"field3\": 1}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null, \n" +
                "\"field2\": 3, \n" +
                "\"field3\": 1}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT SUBSTRING(\"field\", \"field3\", \"field2\" - \"field3\") " +
                                "FROM \"%s\".\"%s\" " +
                                "WHERE SUBSTRING(\"field\", \"field3\", \"field2\" + \"field3\") = 'abcd'",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("ab", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests substring where a conflict with a field exists")
    @ParameterizedTest(name = "testSubstringFieldConflict - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testSubstringFieldConflict(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testSubstringLiteralConflict";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"$100\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"abc\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = SUBSTRING('$1000', 1, 4)",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests substring where a conflict with an operator exists")
    @ParameterizedTest(name = "testSubstringOperatorConflict - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testSubstringOperatorConflict(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testSubstringOperatorConflict";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"$o\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"abc\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = SUBSTRING('$or', 1, 2)",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests FLOOR(... TO ...) in WHERE clause.")
    @ParameterizedTest(name = "testFloorForDateInWhere - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testFloorForDateInWhere(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testFloorForDateInWhere";
        final Instant dateTime = Instant.parse("2020-02-03T12:34:56.78Z");
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime.toEpochMilli()));
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + " WHERE FLOOR(\"field\" TO SECOND) >= \"field\"",
                        getDatabaseName(), tableName));
        Assertions.assertFalse(resultSet.next());

        final ResultSet resultSet2 = statement.executeQuery(
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + " WHERE FLOOR(\"field\" TO SECOND) < \"field\"",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals("101", resultSet2.getString(1));
        Assertions.assertFalse(resultSet2.next());
    }

    @DisplayName("Tests arithmetic functions in WHERE clause.")
    @ParameterizedTest(name = "testArithmeticWhere - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testArithmeticWhere(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereArithmetic";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4, \n" +
                "\"field2\": 3, \n" +
                "\"field3\": 2}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": 2, \n" +
                "\"field2\": 2 \n" +
                "\"field3\": 1}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": 2, \n" +
                "\"field2\": 3, \n" +
                "\"field3\": 1}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null, \n" +
                "\"field2\": 3, \n" +
                "\"field3\": 1}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" * \"field2\" / \"field3\" + \"field2\" - \"field3\" = 7",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests that queries filtering by modulo work.")
    @ParameterizedTest(name = "testModuloWhere - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testModuloWhere(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereModulo";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 5}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": 6}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": 1}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE MOD(\"field\", 3) = 2" +
                                "OR MOD(8, \"field\") = 2" +
                                "OR MOD(3, 2) = \"field\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries containing nested OR.")
    @ParameterizedTest(name = "testNestedOR - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testNestedOR(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "TestWhereOr";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": true, \n" +
                "\"field2\": false, \n" +
                "\"field3\": 5}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": false, \n" +
                "\"field2\": false \n" +
                "\"field3\": 7}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": false, \n" +
                "\"field2\": true, \n" +
                "\"field3\": 1}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": false, \n" +
                "\"field2\": false, \n" +
                "\"field3\": 1}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" OR (\"field2\" OR \"field3\" > 6)",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries with nested AND.")
    @ParameterizedTest(name = "testNestedAND - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testNestedAND(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereAnd";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": true, \n" +
                "\"field2\": true, \n" +
                "\"field3\": 7}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": true, \n" +
                "\"field2\": false \n" +
                "\"field3\": 7}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": false, \n" +
                "\"field2\": true, \n" +
                "\"field3\": 8}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": false, \n" +
                "\"field2\": false, \n" +
                "\"field3\": 1}");

        insertBsonDocuments(tableName,  new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" AND (\"field2\" AND \"field3\" > 6)",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries with nested combined OR and AND.")
    @ParameterizedTest(name = "testNestedOR - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testNestedAndOr(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereAndOrCombined";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": true, \n" +
                "\"field2\": true, \n" +
                "\"field3\": 7, \n" +
                "\"field4\": false}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": true, \n" +
                "\"field2\": false \n" +
                "\"field3\": 7, \n" +
                "\"field4\": false}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": false, \n" +
                "\"field2\": true, \n" +
                "\"field3\": 8, \n" +
                "\"field4\": true}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": false, \n" +
                "\"field2\": false, \n" +
                "\"field3\": 1}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE ((\"field\" AND \"field3\" < 10) AND (\"field2\" OR \"field3\" > 6)) OR \"field4\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries with NOT combined with OR and AND.")
    @ParameterizedTest(name = "testNotCombinedWithAndOr - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testNotCombinedWithAndOr(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereNotAndOr";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": true, \n" +
                "\"field2\": true, \n" +
                "\"field3\": 7, \n" +
                "\"field4\": false}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": false, \n" +
                "\"field2\": false \n" +
                "\"field3\": 7, \n" +
                "\"field4\": false}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": false, \n" +
                "\"field2\": true, \n" +
                "\"field3\": 8, \n" +
                "\"field4\": true}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": false, \n" +
                "\"field2\": false, \n" +
                "\"field3\": 1}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE ((NOT \"field\" AND \"field3\" < 10) AND (NOT \"field2\" OR \"field3\" > 6)) OR \"field4\"",
                        getDatabaseName(), tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("104", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Test queries filtering by CURRENT_DATE.")
    @ParameterizedTest(name = "testWhereCurrentDate - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWhereCurrentDate(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereCurrentDate";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"date\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE CURRENT_DATE > \"date\"",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries filtering by CURRENT_TIME.")
    @ParameterizedTest(name = "testCurrentTime - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testCurrentTime(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereCurrentTime";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"date\": null}");

        insertBsonDocuments(tableName,  new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE CURRENT_TIME <> CAST(\"date\" AS TIME)",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries filtering by CURRENT_TIMESTAMP.")
    @ParameterizedTest(name = "testCurrentTimestamp - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testCurrentTimestamp(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereCurrentTimestamp";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103, \n" +
                "\"date\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE CURRENT_TIMESTAMP <> \"date\"",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries filtering by date extract.")
    @ParameterizedTest(name = "testWhereExtract - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWhereExtract(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereSqlExtract";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date", new BsonDateTime(Instant.parse("2021-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("date", new BsonDateTime(Instant.parse("2020-02-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");
        doc3.append("date", new BsonDateTime(Instant.parse("2020-01-02T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104}");
        doc4.append("date", new BsonDateTime(Instant.parse("2020-01-01T01:00:00.00Z").toEpochMilli()));
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");
        doc5.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:01:00.00Z").toEpochMilli()));
        final BsonDocument doc6 = BsonDocument.parse("{\"_id\": 106}");
        doc6.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:01.00Z").toEpochMilli()));
        final BsonDocument doc7 = BsonDocument.parse("{\"_id\": 107}");
        doc7.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc8 = BsonDocument.parse("{\"_id\": 108, \n" +
                "\"date\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5, doc6, doc7, doc8});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE EXTRACT(YEAR FROM \"date\") = 2021" +
                                "OR EXTRACT(MONTH FROM \"date\") = 2" +
                                "OR EXTRACT(DAY FROM \"date\") = 2" +
                                "OR EXTRACT(HOUR FROM \"date\") = 1" +
                                "OR EXTRACT(MINUTE FROM \"date\") = 1" +
                                "OR EXTRACT(SECOND FROM \"date\") = 1",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("103", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("104", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("105", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("106", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries filtering by DAYNAME")
    @ParameterizedTest(name = "testWhereDayName - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWhereDayName(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereDAYNAME";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date", new BsonDateTime(Instant.parse("2020-01-07T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"date\": null}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE  DAYNAME(\"date\") = 'Tuesday'",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries filtering by MONTHNAME")
    @ParameterizedTest(name = "testWhereMonthName - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWhereMonthName(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereMonthName";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("date", new BsonDateTime(Instant.parse("2020-02-01T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"date\": null}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE  MONTHNAME(\"date\") = 'February'",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("102", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests queries filtering with date diff.")
    @ParameterizedTest(name = "testDateMinus - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testDateMinus(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testWhereDateMinus";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        doc1.append("date2", new BsonDateTime(Instant.parse("2020-01-03T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        doc2.append("date", new BsonDateTime(Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()));
        doc2.append("date2", new BsonDateTime(Instant.parse("2020-01-02T00:00:00.00Z").toEpochMilli()));
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"date\": null}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE TIMESTAMPDIFF(DAY, \"date\", \"date2\") = 2",
                        getDatabaseName(), tableName));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("101", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @DisplayName("Tests that setMaxRows limits the number of rows returned in result set.")
    @ParameterizedTest(name = "testSetMaxRows - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testSetMaxRows(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String collection = "testSetMaxRows";
        final BsonDocument[] documents = new BsonDocument[10];
        final int totalNumberDocuments = 10;
        final int maxRows = 5;
        for (int i = 0; i < totalNumberDocuments; i++) {
            documents[i] = new BsonDocument("field", new BsonString("value"));
        }
        insertBsonDocuments(collection, documents);
        final Statement statement = getDocumentDbStatement();

        // Don't set max rows
        ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), collection));
        int actualRowCount = 0;
        while (resultSet.next()) {
            actualRowCount++;
        }
        Assertions.assertEquals(0, statement.getMaxRows());
        Assertions.assertEquals(totalNumberDocuments, actualRowCount);

        // Set max rows < actual
        statement.setMaxRows(maxRows);
        resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), collection));
        actualRowCount = 0;
        while (resultSet.next()) {
            actualRowCount++;
        }
        Assertions.assertEquals(maxRows, statement.getMaxRows());
        Assertions.assertEquals(maxRows, actualRowCount);

        // Set unlimited
        statement.setMaxRows(0);
        resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), collection));
        actualRowCount = 0;
        while (resultSet.next()) {
            actualRowCount++;
        }
        Assertions.assertEquals(0, statement.getMaxRows());
        Assertions.assertEquals(totalNumberDocuments, actualRowCount);

        // Set max rows > SQL LIMIT
        int limit = maxRows - 1;
        statement.setMaxRows(maxRows);
        resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" LIMIT %d", getDatabaseName(), collection, limit));
        actualRowCount = 0;
        while (resultSet.next()) {
            actualRowCount++;
        }
        Assertions.assertEquals(maxRows, statement.getMaxRows());
        Assertions.assertEquals(limit, actualRowCount);

        // Set max rows < SQL LIMIT
        limit = maxRows + 1;
        statement.setMaxRows(maxRows);
        resultSet = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" LIMIT %d", getDatabaseName(), collection, limit));
        actualRowCount = 0;
        while (resultSet.next()) {
            actualRowCount++;
        }
        Assertions.assertEquals(maxRows, statement.getMaxRows());
        Assertions.assertEquals(maxRows, actualRowCount);

    }
}
