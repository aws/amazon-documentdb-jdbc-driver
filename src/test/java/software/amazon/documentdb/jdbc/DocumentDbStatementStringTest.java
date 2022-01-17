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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DocumentDbStatementStringTest extends DocumentDbStatementTest {

    @DisplayName("Test that queries selecting a substring work with 2 and 3 arguments.")
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

    @DisplayName("Test queries calling CHAR_LENGTH().")
    @ParameterizedTest(name = "testQueryCharLength - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryCharLength(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "tesQueryCharLength";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"abcdefg\"}"); // 7
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"寿司\"}"); // 2
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}"); // 0
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}"); // null
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}"); // null

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT CHAR_LENGTH(\"field\") FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(7, resultSet.getInt(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(2, resultSet.getInt(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(0, resultSet.getInt(1));
            Assertions.assertFalse(resultSet.wasNull());
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getObject(1));
            Assertions.assertTrue(resultSet.wasNull());
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getObject(1));
            Assertions.assertTrue(resultSet.wasNull());
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Test queries calling POSITION with 2 and 3 arguments.")
    @ParameterizedTest(name = "testQueryPosition - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryPosition(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryPosition";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"BanaNa\"}"); // Contains "na" string in 2 places
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"Apple\"}"); // Does not contain "na"
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}"); // Empty string - does not contain "na"
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}"); // Null string
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}"); // Missing string
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            // Test POSITION(%1 IN %2 FROM %3) format with non-null search substring.
            final ResultSet resultSet1 = statement.executeQuery(
                    String.format("SELECT POSITION('na' IN \"field\" FROM 4) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet1);
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals(5, resultSet1.getInt(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals(0, resultSet1.getInt(1));
            Assertions.assertFalse(resultSet1.wasNull());
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals(0, resultSet1.getInt(1));
            Assertions.assertFalse(resultSet1.wasNull());
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertNull(resultSet1.getObject(1));
            Assertions.assertTrue(resultSet1.wasNull());
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertNull(resultSet1.getObject(1));
            Assertions.assertTrue(resultSet1.wasNull());
            Assertions.assertFalse(resultSet1.next());

            // Test POSITION(%1 IN %2) format with non-null search substring.
            final ResultSet resultSet2 = statement.executeQuery(
                    String.format("SELECT POSITION('na' IN \"field\") FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet2);
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals(3, resultSet2.getInt(1));
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals(0, resultSet2.getInt(1));
            Assertions.assertFalse(resultSet2.wasNull());
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals(0, resultSet2.getInt(1));
            Assertions.assertFalse(resultSet2.wasNull());
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertNull(resultSet2.getObject(1));
            Assertions.assertTrue(resultSet2.wasNull());
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertNull(resultSet2.getObject(1));
            Assertions.assertTrue(resultSet2.wasNull());
            Assertions.assertFalse(resultSet2.next());

            // Test POSITION(%1 IN %2 FROM %3) format with negative start index.
            // Returns 0 unless strings are null in which case returns null.
            final ResultSet resultSet3 = statement.executeQuery(
                    String.format("SELECT POSITION('na' IN \"field\" FROM -4) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet3);
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertEquals(0, resultSet3.getInt(1));
            Assertions.assertFalse(resultSet3.wasNull());
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertEquals(0, resultSet3.getInt(1));
            Assertions.assertFalse(resultSet3.wasNull());
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertEquals(0, resultSet3.getInt(1));
            Assertions.assertFalse(resultSet3.wasNull());
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertNull(resultSet3.getObject(1));
            Assertions.assertTrue(resultSet3.wasNull());
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertNull(resultSet3.getObject(1));
            Assertions.assertTrue(resultSet3.wasNull());
            Assertions.assertFalse(resultSet3.next());

            // Test POSITION(%1 IN %2) format with null search substring. Always returns null.
            final ResultSet resultSet4 = statement.executeQuery(
                    String.format("SELECT POSITION(NULL IN \"field\") FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet4);
            while (resultSet4.next()) {
                Assertions.assertNull(resultSet4.getObject(1));
                Assertions.assertTrue(resultSet4.wasNull());
            }
        }
    }

    @DisplayName("Test queries using UPPER().")
    @ParameterizedTest(name = "testQueryUpper - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryUpper(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryUpper";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"Hello World!\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"寿司\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT UPPER(\"field\") FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("HELLO WORLD!", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("寿司", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Test queries using LOWER().")
    @ParameterizedTest(name = "testQueryLower - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryLower(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryLower";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"Hello World!\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"寿司\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT LOWER(\"field\") FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("hello world!", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("寿司", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Test queries using the || operator.")
    @ParameterizedTest(name = "testQueryConcatOperator - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testConcatOperator(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testConcatOperator";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"Hello\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"寿司\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT 'I want to say: ' || \"field\" || '!' FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I want to say: Hello!", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I want to say: 寿司!", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I want to say: !", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Test queries using CONCAT().")
    @ParameterizedTest(name = "testQueryConcatFunction - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testConcatFunction(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testConcatFunction";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"Hello\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"寿司\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT CONCAT('I want to say: ', \"field\", '!') FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I want to say: Hello!", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I want to say: 寿司!", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I want to say: !", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I want to say: !", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I want to say: !", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Test query using combination of different string functions.")
    @ParameterizedTest(name = "testQueryCombinedStringFunctions - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCombinedStringFunctions(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCombinedStringFunctions";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"Hello\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"寿司\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet = statement.executeQuery(
                    String.format("SELECT UPPER(CONCAT('I want to say: ', \"field\", '!'))  "
                                    + "FROM \"%s\".\"%s\""
                                    + "WHERE POSITION('o' IN \"field\") = CHAR_LENGTH(\"field\")",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet);
            // Returns 2 rows: the 1st row with 'Hello' (a proper match)
            // and the 3rd row with empty string since its length is 0 and none found is also 0.
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I WANT TO SAY: HELLO!", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("I WANT TO SAY: !", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Test queries using LEFT().")
    @ParameterizedTest(name = "testQueryLeft - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryLeft(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryLeft";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"Hello World!\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"寿司\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet1 = statement.executeQuery(
                    String.format("SELECT LEFT(\"field\", 5) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet1);
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("Hello", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("寿司", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertNull(resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertNull(resultSet1.getString(1));
            Assertions.assertFalse(resultSet1.next());

            // A negative length always results in null.
            final ResultSet resultSet2 = statement.executeQuery(
                    String.format("SELECT LEFT(\"field\", -2) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet2);
            while (resultSet2.next()) {
                Assertions.assertNull(resultSet2.getObject(1));
                Assertions.assertTrue(resultSet2.wasNull());
            }
        }
    }

    @DisplayName("Test queries using RIGHT().")
    @ParameterizedTest(name = "testQueryRight - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryRight(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryRight";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"field\": \"Hello World!\"}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"field\": \"寿司\"}");
        final BsonDocument doc3 = BsonDocument.parse("{\"_id\": 103,\n" +
                "\"field\": \"\"}");
        final BsonDocument doc4 = BsonDocument.parse("{\"_id\": 104, \n" +
                "\"field\": null}");
        final BsonDocument doc5 = BsonDocument.parse("{\"_id\": 105}");

        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2, doc3, doc4, doc5});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            final ResultSet resultSet1 = statement.executeQuery(
                    String.format("SELECT RIGHT(\"field\", 5) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet1);
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("orld!", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("寿司", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("", resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertNull(resultSet1.getString(1));
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertNull(resultSet1.getString(1));
            Assertions.assertFalse(resultSet1.next());

            // A negative length always results in null.
            final ResultSet resultSet2 = statement.executeQuery(
                    String.format("SELECT RIGHT(\"field\", -2) FROM \"%s\".\"%s\"",
                            getDatabaseName(), tableName));
            Assertions.assertNotNull(resultSet2);
            while (resultSet2.next()) {
                Assertions.assertNull(resultSet2.getObject(1));
                Assertions.assertTrue(resultSet2.wasNull());
            }
        }
    }
}
