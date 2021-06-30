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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;

public class DocumentDbStatementFilterTest extends DocumentDbStatementTest {

    /**
     * Tests that a statement with project, where, group by, having, order, and limit works for a single table.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit works for a single table.")
    @Test
    void testComplexQuery() throws SQLException {
        final String collection = "testComplexQuery";
        final BsonDocument document1 =
                BsonDocument.parse("{ \"_id\" : \"key0\", \"array\": [1, 2, 3, 4, 5] }");
        final BsonDocument document2 =
                BsonDocument.parse("{ \"_id\" : \"key1\", \"array\": [1, 2, 3] }");
        final BsonDocument document3 =
                BsonDocument.parse("{ \"_id\" : \"key2\", \"array\": [1, 2] }");
        final BsonDocument document4 =
                BsonDocument.parse("{ \"_id\" : \"key3\", \"array\": [1, 2, 3, 4, 5] }");
        insertBsonDocuments(
                collection, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document1, document2, document3, document4});
        final Statement statement = getDocumentDbStatement();

        // Verify that result set has correct values.
        statement.execute(String.format(
                "SELECT \"%s\", COUNT(*) AS \"Count\" FROM \"%s\".\"%s\""
                        + "WHERE \"%s\" <> 'key3' "
                        + "GROUP BY \"%s\" HAVING COUNT(*) > 1"
                        + "ORDER BY \"Count\" DESC LIMIT 1",
                collection + "__id",
                DATABASE_NAME, collection + "_array",
                collection + "__id",
                collection + "__id"));
        final ResultSet resultSet1 = statement.getResultSet();
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals("key0", resultSet1.getString(collection + "__id"));
        Assertions.assertEquals(5, resultSet1.getInt("Count"));
        Assertions.assertFalse(resultSet1.next());
    }

    /*
     * Tests that queries with not-equals do not return null or undefined values.
     *
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests that comparisons to null do not return a value.")
    void testComparisonToNull() throws SQLException {
        final String tableName = "testComparisonsToNull";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n}" +
                "\"field\": null");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"field\" <> 5", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests query with two literals in a where clause such that the comparison is true.
     *
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests query WHERE clause containing two literals such that the comparison is true.")
    void testQueryWhereTwoLiteralsTrue() throws SQLException {
        final String tableName = "testQueryWhereTwoLiteralsTrue";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE 2 > 1", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(4, resultSet.getInt(2));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertNull(resultSet.getString(2));
        Assertions.assertFalse(resultSet.next());
    }

    /* Tests that queries with multiple not-equals clauses are correct.
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests that multiple != conditions can be used.")
    void testMultipleNotEquals() throws SQLException {
        final String tableName = "testMultipleNotEquals";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 4}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102, \n" +
                "\"field\": 3}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103, \n" +
                "\"field\": 2}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3, doc4});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"field\" <> 4 AND \"field\" <> 3", DATABASE_NAME, tableName));
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
    @Test
    @DisplayName("Tests query WHERE clause with boolean literal.")
    void testQueryWhereLiteralBoolean() throws SQLException {
        final String tableName = "testQueryWhereLiteralBoolean";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": true, \n " +
                "\"fieldB\": false}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": false, \n " +
                "\"fieldB\": true}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"fieldA\" = TRUE AND \"fieldB\" = FALSE", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(2));
        Assertions.assertFalse(resultSet.next());
    }

    /* Tests that queries CASE are correct, particularly where null or undefined values are involved.
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests queries with CASE and null values are correct.")
    void testCASE() throws SQLException {
        final String tableName = "testCASE";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": 1}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": 2}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": 5}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104,\n" +
                "\"field\": 4}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105,\n" +
                "\"field\": 3}");
        final BsonDocument doc6 = BsonDocument.parse("{\"_id\": 106,\n" +
                "\"field\": null}");
        final BsonDocument doc7 = BsonDocument.parse("{\"_id\": 107}");
        final BsonDocument doc8 = BsonDocument.parse("{\"_id\": 108}");
        doc8.append("field", new BsonMinKey());
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3, doc4, doc5, doc6, doc7, doc8});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CASE " +
                                "WHEN \"field\" < 2  THEN 'A' " +
                                "WHEN \"field\" <= 2 THEN 'B' " +
                                "WHEN \"field\" > 4 THEN 'C' " +
                                "WHEN \"field\" >= 4 THEN 'D' " +
                                "WHEN \"field\" <> 7 THEN 'E' " +
                                "ELSE 'F' END FROM \"%s\".\"%s\"",
                        DATABASE_NAME, tableName));
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
        Assertions.assertEquals("F", resultSet.getString(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("F", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    /**
     * Tests query with a where clause comparing two fields.
     *
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests query with WHERE clause comparing two fields.")
    void testQueryWhereTwoColumns() throws SQLException {
        final String tableName = "testQueryWhereTwoFields";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 4, \n " +
                "\"fieldB\": 5}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": 5, \n " +
                "\"fieldB\": 4}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"fieldA\" < \"fieldB\"", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(4, resultSet.getInt(2));
        Assertions.assertFalse(resultSet.next());
    }

    /* Tests that queries of CASE are correct with two different fields involved.
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests queries with two field CASE.")
    void testCASETwoFields() throws SQLException {
        final String tableName = "testCASETwoFields";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 1,\n" +
                "\"fieldB\": 2}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": 2,\n" +
                "\"fieldB\": 1}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"fieldA\": 1}");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format(
                        "SELECT CASE " +
                                "WHEN \"fieldA\" < \"fieldB\"  THEN 'A' " +
                                "WHEN \"fieldA\" > \"fieldB\" THEN 'B' " +
                                "ELSE 'C' END FROM \"%s\".\"%s\"",
                        DATABASE_NAME, tableName));
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
    @Test
    @DisplayName("Tests queries with nested OR.")
    void testWhereNestedOR() throws SQLException {
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
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" " +
                        "WHERE (\"fieldA\" < 3  OR \"fieldB\" < 2) " +
                        "AND (\"fieldA\" > 12 OR \"fieldB\" > 8)", DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(104, resultSet.getInt(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(105, resultSet.getInt(1));
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    @DisplayName("Tests queries with various types in WHERE clause")
    void testQueryWhereTypes()throws  SQLException {
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
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1, doc2});
        final Statement statement = getDocumentDbStatement();
        final ResultSet resultSet = statement.executeQuery(
                String.format("SELECT * from \"%s\".\"%s\" WHERE \"fieldA\" = 'abc' AND " +
                        "\"fieldB\" = 5 AND \"fieldC\" = TRUE AND \"fieldD\" > '2020-03-11' AND \"fieldE\" = %d AND \"fieldF\" = %f", DATABASE_NAME, tableName, bigInt, doubleValue));
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
    @Test
    @DisplayName("Tests that date literals can be used in WHERE comparisons")
    void testQueryWhereDateLiteral() throws SQLException {
        final String tableName = "testDateLiteral";
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" < DATE '2020-01-02'",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet1.getTimestamp(2));
        Assertions.assertFalse(resultSet1.next());

        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = DATE '2020-01-01'",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet2.getTimestamp(2));
        Assertions.assertFalse(resultSet2.next());

        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" <> DATE '2020-01-01'",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertFalse(resultSet3.next());
    }

    /**
     * Tests that date literals can be used in WHERE comparisons.
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests that timestamp literals can be used in WHERE comparisons")
    void testQueryWhereTimestampLiteral() throws SQLException {
        final String tableName = "testTimestampLiteral";
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" < TIMESTAMP '2020-01-02 00:00:00'",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet1.getTimestamp(2));
        Assertions.assertFalse(resultSet1.next());

        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = TIMESTAMP '2020-01-01 00:00:00'",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet2.getTimestamp(2));
        Assertions.assertFalse(resultSet2.next());

        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"field\" <> TIMESTAMP '2020-01-01 00:00:00'",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertFalse(resultSet3.next());
    }

    /**
     * Tests that calls to EXTRACT can be used in WHERE comparisons.
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests that calls to extract can be used in the WHERE clause.")
    void testQueryWhereExtract() throws SQLException {
        final String tableName = "testWhereExtract";
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE YEAR(\"field\") > 2019",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet1.getTimestamp(2));
        Assertions.assertFalse(resultSet1.next());

        final ResultSet resultSet2 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE MONTH(\"field\") = 2",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertFalse(resultSet2.next());

        final ResultSet resultSet3 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE DAYOFMONTH(\"field\") IN  (1, 2, 3)",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet3);
        Assertions.assertTrue(resultSet3.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet3.getTimestamp(2));
        Assertions.assertFalse(resultSet3.next());
    }

    /**
     * Tests that calls to timestampAdd can be used in WHERE comparisons.
     * @throws SQLException occurs if query fails.
     */
    @Test
    @DisplayName("Tests that timestampadd can be used in WHERE comparisons")
    void testQueryWhereTimestampAdd() throws SQLException {
        final String tableName = "testWhereTimestampAdd";
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD,
                new BsonDocument[]{doc1});
        final Statement statement = getDocumentDbStatement();

        final ResultSet resultSet1 = statement.executeQuery(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE TIMESTAMPADD(DAY, 1, \"field\") = TIMESTAMP '2020-01-02 00:00:00'",
                        DATABASE_NAME, tableName));
        Assertions.assertNotNull(resultSet1);
        Assertions.assertTrue(resultSet1.next());
        Assertions.assertEquals(new Timestamp(dateTime), resultSet1.getTimestamp(2));
        Assertions.assertFalse(resultSet1.next());
    }
}
