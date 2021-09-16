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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DocumentDbStatementJoinTest extends DocumentDbStatementTest {

    /**
     * Test querying for a virtual table from a nested document.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying for a virtual table from a nested document.")
    @ParameterizedTest(name = "testQueryWithTwoLevelDocument - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithTwoLevelDocument(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1 } }");
        insertBsonDocuments(
                "testComplexDocument", new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify the base table.
            final ResultSet resultSet1 = statement.executeQuery(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), "testComplexDocument"));
            Assertions.assertNotNull(resultSet1);

            // Verify the nested table from the field doc.
            final ResultSet resultSet2 = statement.executeQuery(String.format(
                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), "testComplexDocument_doc"));
            Assertions.assertNotNull(resultSet2);
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("key", resultSet2.getString("testComplexDocument__id"));
            Assertions.assertEquals(1, resultSet2.getInt("field"));

            // Verify PROJECT.
            final ResultSet resultSet3 = statement.executeQuery(String.format(
                    "SELECT \"%s\", \"%s\" FROM \"%s\".\"%s\"", "field", "testComplexDocument__id",
                    getDatabaseName(), "testComplexDocument_doc"));
            Assertions.assertNotNull(resultSet3);
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertEquals("key", resultSet3.getString("testComplexDocument__id"));
            Assertions.assertEquals(1, resultSet3.getInt("field"));

            // Verify JOIN on the base table and nested table to produce 3 columns and 1 row.
            final ResultSet resultSet4 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s",
                                    getDatabaseName(),
                                    "testComplexDocument",
                                    getDatabaseName(),
                                    "testComplexDocument_doc",
                                    "\"testComplexDocument\".\"testComplexDocument__id\"",
                                    "\"testComplexDocument_doc\".\"testComplexDocument__id\""));
            Assertions.assertNotNull(resultSet4);
            Assertions.assertEquals(3, resultSet4.getMetaData().getColumnCount());
            int rowCount = 0;
            while (resultSet4.next()) {
                rowCount++;
            }
            Assertions.assertEquals(1, rowCount);
        }
    }

    /**
     * Test querying for a virtual table from a doubly nested document.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying for a virtual table from a doubly nested document.")
    @ParameterizedTest(name = "testQueryWithThreeLevelDocument - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithThreeLevelDocument(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1, \"doc2\" : { \"field2\" : \"value\" } } }");
        insertBsonDocuments(
                "testComplex3LevelDocument", new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify the base table.
            final ResultSet resultSet1 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testComplex3LevelDocument"));
            Assertions.assertNotNull(resultSet1);

            // Verify the nested table from the field doc.
            final ResultSet resultSet2 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testComplex3LevelDocument_doc"));
            Assertions.assertNotNull(resultSet2);

            // Verify the nested table from the field doc2 from the field doc.
            final ResultSet resultSet3 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"",
                                    getDatabaseName(), "testComplex3LevelDocument_doc_doc2"));
            Assertions.assertNotNull(resultSet3);
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertEquals("key", resultSet3.getString("testComplex3LevelDocument__id"));
            Assertions.assertEquals("value", resultSet3.getString("field2"));

            // Verify JOIN on the 3 tables to produce 5 columns and 1 row.
            final ResultSet resultSet4 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s",
                                    getDatabaseName(), "testComplex3LevelDocument",
                                    getDatabaseName(), "testComplex3LevelDocument_doc",
                                    "\"testComplex3LevelDocument\".\"testComplex3LevelDocument__id\"",
                                    "\"testComplex3LevelDocument_doc\".\"testComplex3LevelDocument__id\"",
                                    getDatabaseName(), "testComplex3LevelDocument_doc_doc2",
                                    "\"testComplex3LevelDocument_doc\".\"testComplex3LevelDocument__id\"",
                                    "\"testComplex3LevelDocument_doc_doc2\".\"testComplex3LevelDocument__id\""));
            Assertions.assertNotNull(resultSet4);
            Assertions.assertEquals(5, resultSet4.getMetaData().getColumnCount());
            int rowCount = 0;
            while (resultSet4.next()) {
                rowCount++;
            }
            Assertions.assertEquals(1, rowCount);
        }
    }

    /**
     * Test querying for a virtual table from a nested scalar array.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying for a virtual table from a nested scalar array.")
    @ParameterizedTest(name = "testQueryWithScalarArray - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithScalarArray(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"array\" : [ 1, 2, 3 ] }");
        insertBsonDocuments(
                "testScalarArray", new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify the base table.
            final ResultSet resultSet1 =
                    statement.executeQuery(
                            String.format("SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testScalarArray"));
            Assertions.assertNotNull(resultSet1);

            // Verify the nested table with 3 rows from the field array.
            final ResultSet resultSet2 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testScalarArray_array"));
            Assertions.assertNotNull(resultSet2);
            for (int i = 0; i < 3; i++) {
                Assertions.assertTrue(resultSet2.next());
                Assertions.assertEquals("key", resultSet2.getString("testScalarArray__id"));
                Assertions.assertEquals(i, resultSet2.getLong("array_index_lvl_0"));
                Assertions.assertEquals(i + 1, resultSet2.getInt("value"));
            }

            // Verify JOIN on the base table and nested table to produce 4 columns and 3 rows.
            final ResultSet resultSet3 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s",
                                    getDatabaseName(),
                                    "testScalarArray",
                                    getDatabaseName(),
                                    "testScalarArray_array",
                                    "\"testScalarArray\".\"testScalarArray__id\"",
                                    "\"testScalarArray_array\".\"testScalarArray__id\""));
            Assertions.assertNotNull(resultSet3);
            Assertions.assertEquals(4, resultSet3.getMetaData().getColumnCount());
            int rowCount = 0;
            while (resultSet3.next()) {
                rowCount++;
            }
            Assertions.assertEquals(3, rowCount);
        }
    }

    /**
     * Test querying for a virtual table from a nested array of documents.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests querying for a virtual table from a nested array of documents.")
    @ParameterizedTest(name = "testQueryWithArrayOfDocuments - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithArrayOfDocuments(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        insertBsonDocuments(
                "testDocumentArray", new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify the base table.
            final ResultSet resultSet1 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testDocumentArray"));
            Assertions.assertNotNull(resultSet1);

            // Verify the nested table with 2 rows from the field array.
            final ResultSet resultSet2 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testDocumentArray_array"));
            Assertions.assertNotNull(resultSet2);
            for (int i = 0; i < 2; i++) {
                Assertions.assertTrue(resultSet2.next());
                Assertions.assertEquals("key", resultSet2.getString("testDocumentArray__id"));
                Assertions.assertEquals(i, resultSet2.getLong("array_index_lvl_0"));
                Assertions.assertEquals(i + 1, resultSet2.getInt("field"));
                Assertions.assertEquals("value",
                        resultSet2.getString(i == 0 ? "field1" : "field2"));
                Assertions.assertNull(resultSet2.getString(i == 0 ? "field2" : "field1"));
            }

            // Verify WHERE on the nested table to produce only rows where field = 2.
            final ResultSet resultSet3 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = 2",
                                    getDatabaseName(),
                                    "testDocumentArray_array"));
            Assertions.assertNotNull(resultSet3);
            int rowCount = 0;
            while (resultSet3.next()) {
                Assertions.assertEquals(2, resultSet3.getInt("field"));
                rowCount++;
            }
            Assertions.assertEquals(1, rowCount);

            // Verify JOIN on the base table and nested table to produce 6 columns and 2 rows.
            final ResultSet resultSet4 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s",
                                    getDatabaseName(),
                                    "testDocumentArray",
                                    getDatabaseName(),
                                    "testDocumentArray_array",
                                    "\"testDocumentArray\".\"testDocumentArray__id\"",
                                    "\"testDocumentArray_array\".\"testDocumentArray__id\""));
            Assertions.assertNotNull(resultSet4);
            Assertions.assertEquals(6, resultSet4.getMetaData().getColumnCount());
            rowCount = 0;
            while (resultSet4.next()) {
                rowCount++;
            }
            Assertions.assertEquals(2, rowCount);
        }
    }

    /**
     * Test querying for a virtual table from a 2 level array.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying for a virtual table from a 2 level array.")
    @ParameterizedTest(name = "testQueryWithTwoLevelArray - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithTwoLevelArray(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"array\" : [ [1, 2, 3 ], [ 4, 5, 6 ] ]}");
        insertBsonDocuments(
                "test2LevelArray", new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify the base table.
            final ResultSet resultSet1 =
                    statement.executeQuery(
                            String.format("SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "test2LevelArray"));
            Assertions.assertNotNull(resultSet1);

            // Verify the nested table with 6 rows from the field array.
            final ResultSet resultSet2 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "test2LevelArray_array"));
            Assertions.assertNotNull(resultSet2);
            int expectedValue = 1;
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 3; j++) {
                    Assertions.assertTrue(resultSet2.next());
                    Assertions.assertEquals("key", resultSet2.getString("test2LevelArray__id"));
                    Assertions.assertEquals(i, resultSet2.getLong("array_index_lvl_0"));
                    Assertions.assertEquals(j, resultSet2.getLong("array_index_lvl_1"));
                    Assertions.assertEquals(expectedValue, resultSet2.getInt("value"));
                    expectedValue++;
                }
            }

            // Verify WHERE on the nested table to produce only rows where array_index_lvl_0 is 0.
            final ResultSet resultSet3 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" WHERE \"array_index_lvl_0\" = 0",
                                    getDatabaseName(),
                                    "test2LevelArray_array"));
            Assertions.assertNotNull(resultSet3);
            int rowCount = 0;
            while (resultSet3.next()) {
                Assertions.assertEquals(0, resultSet3.getLong("array_index_lvl_0"));
                rowCount++;
            }
            Assertions.assertEquals(3, rowCount);

            // Verify JOIN on the base table and nested table to produce 5 columns and 6.
            final ResultSet resultSet4 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s",
                                    getDatabaseName(),
                                    "test2LevelArray",
                                    getDatabaseName(),
                                    "test2LevelArray_array",
                                    "\"test2LevelArray\".\"test2LevelArray__id\"",
                                    "\"test2LevelArray_array\".\"test2LevelArray__id\""));
            Assertions.assertNotNull(resultSet4);
            Assertions.assertEquals(5, resultSet4.getMetaData().getColumnCount());
            rowCount = 0;
            while (resultSet4.next()) {
                rowCount++;
            }
            Assertions.assertEquals(6, rowCount);
        }
    }

    /**
     * Test querying for a virtual table from a nested array in a nested document.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying for a virtual table from a nested array in a nested document.")
    @ParameterizedTest(name = "testQueryWithTwoLevelDocumentWithArray - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithTwoLevelDocumentWithArray(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1, \"array\" : [1, 2, 3 ] } }");
        insertBsonDocuments("testComplexDocumentWithArray", new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify the base table.
            final ResultSet resultSet1 =
                    statement.executeQuery(
                            String.format("SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testComplexDocumentWithArray"));
            Assertions.assertNotNull(resultSet1);

            // Verify the nested table from the field doc.
            final ResultSet resultSet2 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testComplexDocumentWithArray_doc"));
            Assertions.assertNotNull(resultSet2);

            // Verify the nested table with 3 rows from the field array in the field doc.
            final ResultSet resultSet3 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testComplexDocumentWithArray_doc_array"));
            Assertions.assertNotNull(resultSet3);
            for (int i = 0; i < 3; i++) {
                Assertions.assertTrue(resultSet3.next());
                Assertions.assertEquals("key",
                        resultSet3.getString("testComplexDocumentWithArray__id"));
                Assertions.assertEquals(i, resultSet3.getLong("doc_array_index_lvl_0"));
                Assertions.assertEquals(i + 1, resultSet3.getInt("value"));
            }

            // Verify WHERE on the nested table to produce only rows where value is 1.
            final ResultSet resultSet4 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" WHERE \"value\" = 1",
                                    getDatabaseName(),
                                    "testComplexDocumentWithArray_doc_array"));
            Assertions.assertNotNull(resultSet4);
            int rowCount = 0;
            while (resultSet4.next()) {
                Assertions.assertEquals(1, resultSet4.getInt("value"));
                rowCount++;
            }
            Assertions.assertEquals(1, rowCount);
            // Verify JOIN on the 3 tables to get 6 columns and 3 rows.
            final ResultSet resultSet5 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s",
                                    getDatabaseName(), "testComplexDocumentWithArray",
                                    getDatabaseName(), "testComplexDocumentWithArray_doc",
                                    "\"testComplexDocumentWithArray\".\"testComplexDocumentWithArray__id\"",
                                    "\"testComplexDocumentWithArray_doc\".\"testComplexDocumentWithArray__id\"",
                                    getDatabaseName(), "testComplexDocumentWithArray_doc_array",
                                    "\"testComplexDocumentWithArray_doc\".\"testComplexDocumentWithArray__id\"",
                                    "\"testComplexDocumentWithArray_doc_array\".\"testComplexDocumentWithArray__id\""));
            Assertions.assertNotNull(resultSet5);
            Assertions.assertEquals(6, resultSet5.getMetaData().getColumnCount());
            rowCount = 0;
            while (resultSet5.next()) {
                rowCount++;
            }
            Assertions.assertEquals(3, rowCount);
        }
    }

    /**
     * Test querying for a virtual table from a nested array in a document in a nested array.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Test querying for a virtual table from a nested array in a document in a nested array.")
    @ParameterizedTest(name = "testQueryWithArrayOfDocumentsWithArrays - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithArrayOfDocumentsWithArrays(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"array2\" : [ 1, 2, 3 ] }, { \"array2\" : [ 4, 5, 6 ] } ]}");
        insertBsonDocuments("testArrayOfDocumentsWithArray", new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify the base table.
            final ResultSet resultSet1 =
                    statement.executeQuery(
                            String.format("SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testArrayOfDocumentsWithArray"));
            Assertions.assertNotNull(resultSet1);

            // Verify the nested table with 2 rows from the field array.
            final ResultSet resultSet2 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testArrayOfDocumentsWithArray_array"));
            Assertions.assertNotNull(resultSet2);

            // Verify the nested table with 6 rows from the field array2 in the documents of array.
            final ResultSet resultSet3 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\"", getDatabaseName(),
                                    "testArrayOfDocumentsWithArray_array_array2"));
            Assertions.assertNotNull(resultSet3);
            int expectedValue = 1;
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 3; j++) {
                    Assertions.assertTrue(resultSet3.next());
                    Assertions.assertEquals("key",
                            resultSet3.getString("testArrayOfDocumentsWithArray__id"));
                    Assertions.assertEquals(i, resultSet3.getLong("array_index_lvl_0"));
                    Assertions.assertEquals(j, resultSet3.getLong("array_array2_index_lvl_0"));
                    Assertions.assertEquals(expectedValue, resultSet3.getInt("value"));
                    expectedValue++;
                }
            }

            // Verify WHERE on the array2 nested table to produce only rows where array_index_lvl_0 is 0.
            final ResultSet resultSet4 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" WHERE \"array_index_lvl_0\" = 0",
                                    getDatabaseName(),
                                    "testArrayOfDocumentsWithArray_array_array2"));
            Assertions.assertNotNull(resultSet4);
            int rowCount = 0;
            while (resultSet4.next()) {
                Assertions.assertEquals(0, resultSet4.getLong("array_index_lvl_0"));
                rowCount++;
            }
            Assertions.assertEquals(3, rowCount);
            Assertions.assertFalse(resultSet4.next());

            // Verify JOIN on the 3 tables to get 7 columns and 6 rows.
            final ResultSet resultSet5 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON %s = %s "
                                            + "AND %s = %s",
                                    getDatabaseName(), "testArrayOfDocumentsWithArray",
                                    getDatabaseName(), "testArrayOfDocumentsWithArray_array",
                                    "\"testArrayOfDocumentsWithArray\".\"testArrayOfDocumentsWithArray__id\"",
                                    "\"testArrayOfDocumentsWithArray_array\".\"testArrayOfDocumentsWithArray__id\"",
                                    getDatabaseName(), "testArrayOfDocumentsWithArray_array_array2",
                                    "\"testArrayOfDocumentsWithArray_array\".\"testArrayOfDocumentsWithArray__id\"",
                                    "\"testArrayOfDocumentsWithArray_array_array2\".\"testArrayOfDocumentsWithArray__id\"",
                                    "\"testArrayOfDocumentsWithArray_array\".\"array_index_lvl_0\"",
                                    "\"testArrayOfDocumentsWithArray_array_array2\".\"array_index_lvl_0\""));
            Assertions.assertNotNull(resultSet5);
            Assertions.assertEquals(7, resultSet5.getMetaData().getColumnCount());
            rowCount = 0;
            while (resultSet5.next()) {
                rowCount++;
            }
            Assertions.assertEquals(6, rowCount);
        }
    }

    /**
     * Tests that a statement with project, where, group by, having, order, and limit works with same collection joins.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit works with same collection joins.")
    @ParameterizedTest(name = "testComplexQueryWithSameCollectionJoin - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testComplexQueryWithSameCollectionJoin(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String collection = "testComplexQueryJoin";
        final BsonDocument document1 =
                BsonDocument.parse("{ \"_id\" : \"key0\", \"field\": 0, \"array\": [1, 2, 3, 4, 5] }");
        final BsonDocument document2 =
                BsonDocument.parse("{ \"_id\" : \"key1\", \"field\": 0, \"array\": [1, 2, 3] }");
        final BsonDocument document3 =
                BsonDocument.parse("{ \"_id\" : \"key2\", \"field\": 0, \"array\": [1, 2] }");
        final BsonDocument document4 =
                BsonDocument.parse("{ \"_id\" : \"key3\", \"field\": 1, \"array\": [1, 2, 3, 4, 5] }");
        insertBsonDocuments(collection, new BsonDocument[]{document1, document2, document3, document4});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            // Verify that result set has correct values.
            statement.execute(String.format(
                    "SELECT SUM(\"%s\") as \"Sum\", COUNT(*) AS \"Count\" FROM \"%s\".\"%s\""
                            + "INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\""
                            + "WHERE \"%s\" <> 1 "
                            + "GROUP BY \"%s\".\"%s\" HAVING COUNT(*) > 1"
                            + "ORDER BY \"Count\" DESC LIMIT 1",
                    "field",
                    getDatabaseName(), collection,
                    getDatabaseName(), collection + "_array",
                    collection, collection + "__id",
                    collection + "_array", collection + "__id",
                    "field",
                    collection, collection + "__id"));
            final ResultSet resultSet1 = statement.getResultSet();
            Assertions.assertNotNull(resultSet1);
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals(0, resultSet1.getInt("Sum"));
            Assertions.assertEquals(5, resultSet1.getInt("Count"));
            Assertions.assertFalse(resultSet1.next());
        }
    }

    /**
     * Tests that different join types produce the correct result for tables from same collection.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests that different join types produce the correct result for tables from same collection.")
    @ParameterizedTest(name = "testJoinTypesForTablesFromSameCollection - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testJoinTypesForTablesFromSameCollection(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String collection = "testSameCollectionJoin";
        final BsonDocument document1 = BsonDocument.parse("{ \"_id\" : \"key0\", \"doc1\": { \"field\" : 1 } }");
        final BsonDocument document2 = BsonDocument.parse("{ \"_id\" : \"key1\", \"doc2\": { \"field\": 2 } }");
        insertBsonDocuments(collection, new BsonDocument[]{document1, document2});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            // Verify that an inner join will return an empty result set.
            statement.execute(String.format(
                    "SELECT * FROM \"%s\".\"%s\" INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                    getDatabaseName(), collection + "_doc1",
                    getDatabaseName(), collection + "_doc2",
                    collection + "_doc1", collection + "__id",
                    collection + "_doc2", collection + "__id"));
            final ResultSet resultSet1 = statement.getResultSet();
            Assertions.assertNotNull(resultSet1);
            Assertions.assertFalse(resultSet1.next());

            // Verify that a left outer join will return 1 row.
            statement.execute(String.format(
                    "SELECT * FROM \"%s\".\"%s\" LEFT JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                    getDatabaseName(), collection + "_doc1",
                    getDatabaseName(), collection + "_doc2",
                    collection + "_doc1", collection + "__id",
                    collection + "_doc2", collection + "__id"));
            final ResultSet resultSet2 = statement.getResultSet();
            Assertions.assertNotNull(resultSet2);
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("key0", resultSet2.getString(collection + "__id"));
            Assertions.assertEquals(1, resultSet2.getInt("field"));
            Assertions.assertNull(resultSet2.getString(collection + "__id0"));
            Assertions.assertEquals(0, resultSet2.getInt("field0"));
            Assertions.assertFalse(resultSet2.next());
        }
    }

    @Disabled("Incorrect behaviour for right or full joins involving more than 2 virtual tables.")
    @DisplayName("Tests behaviour of right join for tables from the same collection.")
    @ParameterizedTest(name = "testRightJoinForTablesFromSameCollection - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testRightJoinForTablesFromSameCollection(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String collection = "testSameCollectionRightJoin";
        final BsonDocument document1 =
                BsonDocument.parse(
                        "{ \"_id\" : \"key0\", \"doc1\": { \"field\" : 1 }, \"doc2\": { \"field\": 2 }}");
        final BsonDocument document2 = BsonDocument.parse("{ \"_id\" : \"key1\", \"doc2\": { \"field\": 2 } }");
        insertBsonDocuments(collection, new BsonDocument[]{document1, document2});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            // Verify that a right outer join will return 1 rows.
            statement.execute(String.format(
                    "SELECT * FROM \"%s\".\"%s\" RIGHT JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                    getDatabaseName(), collection,
                    getDatabaseName(), collection + "_doc1",
                    collection, collection + "__id",
                    collection + "_doc1", collection + "__id"));
            final ResultSet resultSet = statement.getResultSet();
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertFalse(resultSet.next());

            // Verify that an inner join combined with a right outer join will return 2 rows.
            statement.execute(
                    String.format(
                            "SELECT * FROM \"%s\".\"%s\" "
                                    + "INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\""
                                    + "RIGHT JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                            getDatabaseName(),
                            collection,
                            getDatabaseName(),
                            collection + "_doc1",
                            collection,
                            collection + "__id",
                            collection + "_doc1",
                            collection + "__id",
                            getDatabaseName(),
                            collection + "_doc2",
                            collection + "_doc1",
                            collection + "__id",
                            collection + "_doc2",
                            collection + "__id"));
            final ResultSet resultSet2 = statement.getResultSet();
            Assertions.assertNotNull(resultSet2);
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertFalse(resultSet2.next());
        }
    }

    /**
     * Tests that a statement with project, where, group by, having, order, and limit works with same collection joins.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Disabled("Relies on $lookup with pipeline.")
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit works with a different collection join.")
    @ParameterizedTest(name = "testComplexQueryWithDifferentCollectionJoin - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testComplexQueryWithDifferentCollectionJoin(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String collection1 = "testComplexQueryDifferentCollectionJoin1";
        final String collection2 = "testComplexQueryDifferentCollectionJoin2";
        final BsonDocument document1 =
                BsonDocument.parse("{ \"_id\" : \"key0\",  \"array\": [1, 2, 3, 4, 5] }");
        final BsonDocument document2 =
                BsonDocument.parse("{ \"_id\" : \"key1\",  \"array\": [1, 2, 3, 4] }");
        final BsonDocument document3 =
                BsonDocument.parse("{ \"_id\" : \"key2\",  \"array\": [1, 2, 3] }");
        final BsonDocument document4 =
                BsonDocument.parse("{ \"_id\" : \"key3\", \"array\": [1, 2, 3, 4 ] }");
        final BsonDocument document5 =
                BsonDocument.parse("{ \"_id\" : \"key0\", \"field\": 1, \"field2\" : 0 }");
        final BsonDocument document6 =
                BsonDocument.parse("{ \"_id\" : \"key1\", \"field\": 0, \"field2\" : 1 }");
        final BsonDocument document7 =
                BsonDocument.parse("{ \"_id\" : \"key2\", \"field\": 0,  \"field2\": 0 }");
        insertBsonDocuments(
                collection1, new BsonDocument[]{document1, document2, document3, document4});
        insertBsonDocuments(
                collection2, new BsonDocument[]{document5, document6, document7});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            // Verify that result set has correct values. Expecting query to single out document3.
            statement.execute(String.format(
                    "SELECT \"%s\", SUM(\"%s\") as \"Sum\", COUNT(*) AS \"Count\" FROM \"%s\".\"%s\""
                            + "INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\""
                            + "WHERE \"%s\" <> 1 "
                            + "GROUP BY \"%s\".\"%s\" HAVING COUNT(*) < 5"
                            + "ORDER BY \"Count\" DESC LIMIT 1",
                    collection2 + "__id",
                    "field",
                    getDatabaseName(), collection1 + "_array",
                    getDatabaseName(), collection2,
                    collection1 + "_array", collection1 + "__id",
                    collection2, collection2 + "__id",
                    "field2",
                    collection2, collection2 + "__id"));
            final ResultSet resultSet1 = statement.getResultSet();
            Assertions.assertNotNull(resultSet1);
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals("key2", resultSet1.getString(collection2 + "__id"));
            Assertions.assertEquals(0, resultSet1.getInt("Sum"));
            Assertions.assertEquals(3, resultSet1.getInt("Count"));
            Assertions.assertFalse(resultSet1.next());
        }
    }

    /**
     * "Tests that different join types produce the correct result for tables from different collections.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Disabled("Relies on $lookup with pipeline.")
    @DisplayName("Tests that different join types produce the correct result for tables from different collections.")
    @ParameterizedTest(name = "testJoinTypesForTablesFromDifferentCollection - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testJoinTypesForTablesFromDifferentCollection() throws SQLException {
        final String collection1 = "testDifferentCollectionJoin1";
        final String collection2 = "testDifferentCollectionJoin2";
        final BsonDocument document1 =
                BsonDocument.parse(
                        "{ \"_id\" : \"key0\", \"array\": [ {\"field\": 1, \"field2\": \"value\"}, {\"field\": 2, \"field2\": \"value2\"}] }");
        final BsonDocument document2 = BsonDocument.parse(
                "{ \"_id\" : \"key1\", \"doc\": { \"field\": 1, field3: \"value3\"} }");
        insertBsonDocuments(
                collection1, new BsonDocument[]{document1});
        insertBsonDocuments(
                collection2, new BsonDocument[]{document2});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);

            // Verify that an inner join will return 1 row where field0 = field.
            statement.execute(String.format(
                    "SELECT * FROM \"%s\".\"%s\" INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                    getDatabaseName(), collection1 + "_array",
                    getDatabaseName(), collection2 + "_doc",
                    collection1 + "_array", "field",
                    collection2 + "_doc", "field"));
            final ResultSet resultSet1 = statement.getResultSet();
            Assertions.assertNotNull(resultSet1);
            Assertions.assertTrue(resultSet1.next());
            Assertions.assertEquals(resultSet1.getInt("field"), resultSet1.getInt("field0"));
            Assertions.assertEquals("key0", resultSet1.getString(collection1 + "__id"));
            Assertions.assertEquals("key1", resultSet1.getString(collection2 + "__id"));
            Assertions.assertFalse(resultSet1.next());

            // Verify that a left outer join will return 2 rows but only 1 match from the right.
            statement.execute(String.format(
                    "SELECT * FROM \"%s\".\"%s\" LEFT JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                    getDatabaseName(), collection1 + "_array",
                    getDatabaseName(), collection2 + "_doc",
                    collection1 + "_array", "field",
                    collection2 + "_doc", "field"));
            final ResultSet resultSet2 = statement.getResultSet();
            Assertions.assertNotNull(resultSet2);
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals(resultSet2.getInt("field"), resultSet2.getInt("field0"));
            Assertions.assertEquals("key0", resultSet2.getString(collection1 + "__id"));
            Assertions.assertEquals("key1", resultSet2.getString(collection2 + "__id"));
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("key0", resultSet2.getString(collection1 + "__id"));
            Assertions.assertNull(resultSet2.getString(collection2 + "__id"));
            Assertions.assertEquals(2, resultSet2.getInt("field"));
            Assertions.assertEquals(0, resultSet2.getInt("field0"));
            Assertions.assertFalse(resultSet2.next());
        }
    }

    /**
     * Test querying using projection a three-level document.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests querying with projects on a three-level document. Addresses AD-115.")
    @ParameterizedTest(name = "testProjectionQueryWithThreeLevelDocument - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testProjectionQueryWithThreeLevelDocument(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testProjectionQueryWithThreeLevelDocument";
        final String keyColumnName = tableName + "__id";
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1, \"doc2\" : { \"field2\" : \"value\" } } }");
        insertBsonDocuments(tableName, new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify the nested table from the field doc2 from the field doc.
            final ResultSet resultSet2 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT \"%s__id\" FROM \"%s\".\"%s\"",
                                    tableName, getDatabaseName(), tableName + "_doc"));
            Assertions.assertNotNull(resultSet2);
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("key", resultSet2.getString(keyColumnName));

            // Verify the nested table from the field doc2 from the field doc.
            final ResultSet resultSet3 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT \"%s__id\" FROM \"%s\".\"%s\"",
                                    tableName, getDatabaseName(), tableName + "_doc_doc2"));
            Assertions.assertNotNull(resultSet3);
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertEquals("key", resultSet3.getString(keyColumnName));

            // Verify JOIN on the 3 tables to produce 2 columns and 1 row.
            final ResultSet resultSet4 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT \"%s\".\"%s__id\", \"field2\" FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON \"%s\".\"%s\" = \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                                    tableName,
                                    tableName,
                                    getDatabaseName(),
                                    tableName,
                                    getDatabaseName(),
                                    tableName + "_doc",
                                    tableName,
                                    keyColumnName,
                                    tableName + "_doc",
                                    keyColumnName,
                                    getDatabaseName(),
                                    tableName + "_doc_doc2",
                                    tableName + "_doc",
                                    keyColumnName,
                                    tableName + "_doc_doc2",
                                    keyColumnName));
            Assertions.assertNotNull(resultSet4);
            Assertions.assertEquals(2, resultSet4.getMetaData().getColumnCount());
            int rowCount = 0;
            while (resultSet4.next()) {
                rowCount++;
            }
            Assertions.assertEquals(1, rowCount);
        }
    }


    /**
     * Tests queries with natural joins where there are no matching fields other than ID.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests queries with natural joins.")
    @ParameterizedTest(name = "testNaturalJoin - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testNaturalJoin(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testNaturalJoin";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 10, " +
                "\"sub\": {" +
                "   \"subField\": 15}}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * from \"%s\".\"%s\" NATURAL JOIN \"%s\".\"%s\"",
                            getDatabaseName(), tableName, getDatabaseName(), tableName + "_sub"));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("101", resultSet.getString(1));
            Assertions.assertEquals(10, resultSet.getInt(2));
            Assertions.assertEquals(15, resultSet.getInt(3));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that natural joins where there is an additional matching column works.
     * @throws SQLException occurs if query or connection fails.
     */
    @Disabled("Only joins on foreign keys are supported currently.")
    @DisplayName("Tests queries with natural join where an additional column matches the sub-table.")
    @ParameterizedTest(name = "testNaturalJoinWithExtraColumn - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testNaturalJoinWithExtraColumn(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testNaturalJoinWithExtraColumn";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 10, " +
                "\"sub\": {" +
                "   \"subField\": 15," +
                "   \"fieldA\": 10}}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * from \"%s\".\"%s\" NATURAL JOIN \"%s\".\"%s\"",
                            getDatabaseName(), tableName, getDatabaseName(), tableName + "_sub"));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("101", resultSet.getString(1));
            Assertions.assertEquals(10, resultSet.getInt(2));
            Assertions.assertEquals(15, resultSet.getInt(3));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that a cross join with a WHERE clause matching IDs works.
     * @throws SQLException occurs if query or connection fails.
     */
    @DisplayName("Tests basic cross-join with WHERE condition.")
    @ParameterizedTest(name = "testCrossJoinBasic - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCrossJoinBasic(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCrossJoinBasic";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 10, " +
                "\"sub\": {" +
                "   \"subField\": 15," +
                "   \"fieldA\": 10}}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * from \"%s\".\"%s\" CROSS JOIN \"%s\".\"%s\" WHERE " +
                                    "\"testCrossJoinBasic\".\"testCrossJoinBasic__id\" = \"testCrossJoinBasic_sub\".\"testCrossJoinBasic__id\"",
                            getDatabaseName(), tableName, getDatabaseName(), tableName + "_sub"));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("101", resultSet.getString(1));
            Assertions.assertEquals(10, resultSet.getInt(2));
            Assertions.assertEquals("101", resultSet.getString(3));
            Assertions.assertEquals(15, resultSet.getInt(4));
            Assertions.assertFalse(resultSet.next());
        }
    }

    /**
     * Tests that a cross join works.
     * @throws SQLException occurs if query or connection fails.
     */
    @Disabled("Only joins on foreign keys are supported currently.")
    @DisplayName("Tests cross-join without WHERE condition.")
    @ParameterizedTest(name = "testCrossJoin - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testCrossJoin(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testCrossJoin";
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101,\n" +
                "\"fieldA\": 10, " +
                "\"sub\": {" +
                "   \"subField\": 15," +
                "   \"fieldA\": 10}}");
        final BsonDocument doc2 = BsonDocument.parse("{\"_id\": 102,\n" +
                "\"fieldA\": 10, " +
                "\"sub\": {" +
                "   \"subField\": 15," +
                "   \"fieldA\": 10}}");
        insertBsonDocuments(tableName, new BsonDocument[]{doc1, doc2});
        try (Connection connection = getConnection()) {
            final Statement statement = getDocumentDbStatement(connection);
            final ResultSet resultSet = statement.executeQuery(
                    String.format(
                            "SELECT * from \"%s\".\"%s\" CROSS JOIN \"%s\".\"%s\"",
                            getDatabaseName(), tableName, getDatabaseName(), tableName + "_sub"));
            Assertions.assertNotNull(resultSet);
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("101", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("101", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("102", resultSet.getString(1));
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("102", resultSet.getString(1));
            Assertions.assertFalse(resultSet.next());
        }
    }

    @DisplayName("Tests joins with array of two level documents.")
    @ParameterizedTest(name = "testQueryWithArrayOfTwoLevelDocuments - [{index}] - {arguments}")
    @MethodSource({"getTestEnvironments"})
    void testQueryWithArrayOfTwoLevelDocuments(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        setTestEnvironment(testEnvironment);
        final String tableName = "testQueryWithArrayOfTwoLevelDocuments";
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": { \"field2\" : 2, \"field3\" : \"value\" } }, { \"field\" : 1 } ]}");
        insertBsonDocuments(
                tableName, new BsonDocument[]{document});
        try (Connection connection = getConnection()) {
            final DocumentDbStatement statement = getDocumentDbStatement(connection);

            // Verify LEFT OUTER JOIN on the nested table and 2nd nested table to produce 2 rows.
            final ResultSet resultSet1 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "LEFT OUTER JOIN \"%s\".\"%s\" "
                                            + "ON \"%s\".\"%s\" = \"%s\".\"%s\" "
                                            + "AND \"%s\".\"%s\" = \"%s\".\"%s\"",
                                    getDatabaseName(),
                                    tableName + "_array",
                                    getDatabaseName(),
                                    tableName + "_array_field1",
                                    tableName + "_array",
                                    tableName + "__id",
                                    tableName + "_array_field1",
                                    tableName + "__id",
                                    tableName + "_array",
                                    "array_index_lvl_0",
                                    tableName + "_array_field1",
                                    "array_index_lvl_0"));
            Assertions.assertNotNull(resultSet1);
            int rowCount = 0;
            while (resultSet1.next()) {
                rowCount++;
            }
            Assertions.assertEquals(2, rowCount);

            // Verify INNER JOIN on the nested table and 2nd nested table to produce 1 row.
            final ResultSet resultSet2 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "INNER JOIN \"%s\".\"%s\" "
                                            + "ON \"%s\".\"%s\" = \"%s\".\"%s\" "
                                            + "AND \"%s\".\"%s\" = \"%s\".\"%s\"",
                                    getDatabaseName(),
                                    tableName + "_array",
                                    getDatabaseName(),
                                    tableName + "_array_field1",
                                    tableName + "_array",
                                    tableName + "__id",
                                    tableName + "_array_field1",
                                    tableName + "__id",
                                    tableName + "_array",
                                    "array_index_lvl_0",
                                    tableName + "_array_field1",
                                    "array_index_lvl_0"));
            Assertions.assertNotNull(resultSet2);
            rowCount = 0;
            while (resultSet2.next()) {
                Assertions.assertEquals("key", resultSet2.getString(tableName + "__id0"));
                Assertions.assertEquals(0, resultSet2.getInt("array_index_lvl_00"));
                rowCount++;
            }
            Assertions.assertEquals(1, rowCount);

            // Verify LEFT OUTER JOIN on the nested table and 2nd nested table with filter to produce 1 row.
            final ResultSet resultSet3 =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM \"%s\".\"%s\" "
                                            + "LEFT OUTER JOIN \"%s\".\"%s\" "
                                            + "ON \"%s\".\"%s\" = \"%s\".\"%s\" "
                                            + "AND \"%s\".\"%s\" = \"%s\".\"%s\""
                                            + "WHERE \"%s\".\"%s\" IS NULL "
                                            + "AND \"%s\".\"%s\" IS NULL ",
                                    getDatabaseName(),
                                    tableName + "_array",
                                    getDatabaseName(),
                                    tableName + "_array_field1",
                                    tableName + "_array",
                                    tableName + "__id",
                                    tableName + "_array_field1",
                                    tableName + "__id",
                                    tableName + "_array",
                                    "array_index_lvl_0",
                                    tableName + "_array_field1",
                                    "array_index_lvl_0",
                                    tableName + "_array_field1",
                                    tableName + "__id",
                                    tableName + "_array_field1",
                                    "array_index_lvl_0"));
            Assertions.assertNotNull(resultSet3);
            rowCount = 0;
            while (resultSet3.next()) {
                Assertions.assertNull(resultSet3.getString(tableName + "__id0"));
                rowCount++;
            }
            Assertions.assertEquals(1, rowCount);
        }
    }
}
