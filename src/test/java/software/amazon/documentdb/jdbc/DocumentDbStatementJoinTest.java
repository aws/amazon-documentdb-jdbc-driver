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
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DocumentDbStatementJoinTest extends DocumentDbStatementTest {

    /**
     * Test querying for a virtual table from a nested document.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testQueryWithTwoLevelDocument() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1 } }");
        insertBsonDocuments(
                "testComplexDocument", DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document});
        final DocumentDbStatement statement = getDocumentDbStatement();

        // Verify the base table.
        final ResultSet resultSet1 = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, "testComplexDocument"));
        Assertions.assertNotNull(resultSet1);

        // Verify the nested table from the field doc.
        final ResultSet resultSet2 = statement.executeQuery(String.format(
                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, "testComplexDocument_doc"));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals("key", resultSet2.getString("testComplexDocument__id"));
        Assertions.assertEquals(1, resultSet2.getInt("field"));

        // Verify PROJECT.
        final ResultSet resultSet3 = statement.executeQuery(String.format(
                "SELECT \"%s\", \"%s\" FROM \"%s\".\"%s\"", "field", "testComplexDocument__id", DATABASE_NAME, "testComplexDocument_doc"));
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
                                DATABASE_NAME,
                                "testComplexDocument",
                                DATABASE_NAME,
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

    /**
     * Test querying for a virtual table from a doubly nested document.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testQueryWithThreeLevelDocument() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1, \"doc2\" : { \"field2\" : \"value\" } } }");
        insertBsonDocuments(
                "testComplex3LevelDocument", DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document});
        final DocumentDbStatement statement = getDocumentDbStatement();

        // Verify the base table.
        final ResultSet resultSet1 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testComplex3LevelDocument"));
        Assertions.assertNotNull(resultSet1);

        // Verify the nested table from the field doc.
        final ResultSet resultSet2 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testComplex3LevelDocument_doc"));
        Assertions.assertNotNull(resultSet2);

        // Verify the nested table from the field doc2 from the field doc.
        final ResultSet resultSet3 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"",
                                DATABASE_NAME, "testComplex3LevelDocument_doc_doc2"));
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
                                DATABASE_NAME, "testComplex3LevelDocument",
                                DATABASE_NAME, "testComplex3LevelDocument_doc",
                                "\"testComplex3LevelDocument\".\"testComplex3LevelDocument__id\"",
                                "\"testComplex3LevelDocument_doc\".\"testComplex3LevelDocument__id\"",
                                DATABASE_NAME, "testComplex3LevelDocument_doc_doc2",
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

    /**
     * Test querying for a virtual table from a nested scalar array.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testQueryWithScalarArray() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"array\" : [ 1, 2, 3 ] }");
        insertBsonDocuments(
                "testScalarArray", DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document});
        final DocumentDbStatement statement = getDocumentDbStatement();

        // Verify the base table.
        final ResultSet resultSet1 =
                statement.executeQuery(
                        String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testScalarArray"));
        Assertions.assertNotNull(resultSet1);

        // Verify the nested table with 3 rows from the field array.
        final ResultSet resultSet2 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
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
                                DATABASE_NAME,
                                "testScalarArray",
                                DATABASE_NAME,
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

    /**
     * Test querying for a virtual table from a nested array of documents.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testQueryWithArrayOfDocuments() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        insertBsonDocuments(
                "testDocumentArray", DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document});
        final DocumentDbStatement statement = getDocumentDbStatement();

        // Verify the base table.
        final ResultSet resultSet1 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testDocumentArray"));
        Assertions.assertNotNull(resultSet1);

        // Verify the nested table with 2 rows from the field array.
        final ResultSet resultSet2 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testDocumentArray_array"));
        Assertions.assertNotNull(resultSet2);
        for (int i = 0; i < 2; i++) {
            Assertions.assertTrue(resultSet2.next());
            Assertions.assertEquals("key", resultSet2.getString("testDocumentArray__id"));
            Assertions.assertEquals(i, resultSet2.getLong("array_index_lvl_0"));
            Assertions.assertEquals(i + 1, resultSet2.getInt("field"));
            Assertions.assertEquals("value", resultSet2.getString(i == 0 ? "field1" : "field2"));
            Assertions.assertNull(resultSet2.getString(i == 0 ? "field2" : "field1"));
        }

        // Verify WHERE on the nested table to produce only rows where field = 2.
        final ResultSet resultSet3 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" = 2", DATABASE_NAME,
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
                                DATABASE_NAME,
                                "testDocumentArray",
                                DATABASE_NAME,
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

    /**
     * Test querying for a virtual table from a 2 level array.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testQueryWithTwoLevelArray() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"array\" : [ [1, 2, 3 ], [ 4, 5, 6 ] ]}");
        insertBsonDocuments(
                "test2LevelArray", DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document});
        final DocumentDbStatement statement = getDocumentDbStatement();

        // Verify the base table.
        final ResultSet resultSet1 =
                statement.executeQuery(
                        String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "test2LevelArray"));
        Assertions.assertNotNull(resultSet1);

        // Verify the nested table with 6 rows from the field array.
        final ResultSet resultSet2 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
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
                                "SELECT * FROM \"%s\".\"%s\" WHERE \"array_index_lvl_0\" = 0", DATABASE_NAME,
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
                                DATABASE_NAME,
                                "test2LevelArray",
                                DATABASE_NAME,
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

    /**
     * Test querying for a virtual table from a nested array in a nested document.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testQueryWithTwoLevelDocumentWithArray() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1, \"array\" : [1, 2, 3 ] } }");
        insertBsonDocuments(
                "testComplexDocumentWithArray",
                DATABASE_NAME,
                USER,
                PASSWORD,
                new BsonDocument[]{document});
        final DocumentDbStatement statement = getDocumentDbStatement();

        // Verify the base table.
        final ResultSet resultSet1 =
                statement.executeQuery(
                        String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testComplexDocumentWithArray"));
        Assertions.assertNotNull(resultSet1);

        // Verify the nested table from the field doc.
        final ResultSet resultSet2 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testComplexDocumentWithArray_doc"));
        Assertions.assertNotNull(resultSet2);

        // Verify the nested table with 3 rows from the field array in the field doc.
        final ResultSet resultSet3 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testComplexDocumentWithArray_doc_array"));
        Assertions.assertNotNull(resultSet3);
        for (int i = 0; i < 3; i++) {
            Assertions.assertTrue(resultSet3.next());
            Assertions.assertEquals("key", resultSet3.getString("testComplexDocumentWithArray__id"));
            Assertions.assertEquals(i, resultSet3.getLong("doc_array_index_lvl_0"));
            Assertions.assertEquals(i + 1, resultSet3.getInt("value"));
        }

        // Verify WHERE on the nested table to produce only rows where value is 1.
        final ResultSet resultSet4 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\" WHERE \"value\" = 1", DATABASE_NAME,
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
                                DATABASE_NAME, "testComplexDocumentWithArray",
                                DATABASE_NAME, "testComplexDocumentWithArray_doc",
                                "\"testComplexDocumentWithArray\".\"testComplexDocumentWithArray__id\"",
                                "\"testComplexDocumentWithArray_doc\".\"testComplexDocumentWithArray__id\"",
                                DATABASE_NAME, "testComplexDocumentWithArray_doc_array",
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

    /**
     * Test querying for a virtual table from a nested array in a document in a nested array.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    void testQueryWithArrayOfDocumentsWithArrays() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"array2\" : [ 1, 2, 3 ] }, { \"array2\" : [ 4, 5, 6 ] } ]}");
        insertBsonDocuments(
                "testArrayOfDocumentsWithArray",
                DATABASE_NAME,
                USER,
                PASSWORD,
                new BsonDocument[]{document});
        final DocumentDbStatement statement = getDocumentDbStatement();

        // Verify the base table.
        final ResultSet resultSet1 =
                statement.executeQuery(
                        String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testArrayOfDocumentsWithArray"));
        Assertions.assertNotNull(resultSet1);

        // Verify the nested table with 2 rows from the field array.
        final ResultSet resultSet2 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testArrayOfDocumentsWithArray_array"));
        Assertions.assertNotNull(resultSet2);

        // Verify the nested table with 6 rows from the field array2 in the documents of array.
        final ResultSet resultSet3 =
                statement.executeQuery(
                        String.format(
                                "SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME,
                                "testArrayOfDocumentsWithArray_array_array2"));
        Assertions.assertNotNull(resultSet3);
        int expectedValue = 1;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 3; j++) {
                Assertions.assertTrue(resultSet3.next());
                Assertions.assertEquals("key", resultSet3.getString("testArrayOfDocumentsWithArray__id"));
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
                                "SELECT * FROM \"%s\".\"%s\" WHERE \"array_index_lvl_0\" = 0", DATABASE_NAME,
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
                                DATABASE_NAME, "testArrayOfDocumentsWithArray",
                                DATABASE_NAME, "testArrayOfDocumentsWithArray_array",
                                "\"testArrayOfDocumentsWithArray\".\"testArrayOfDocumentsWithArray__id\"",
                                "\"testArrayOfDocumentsWithArray_array\".\"testArrayOfDocumentsWithArray__id\"",
                                DATABASE_NAME, "testArrayOfDocumentsWithArray_array_array2",
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

    /**
     * Tests that a statement with project, where, group by, having, order, and limit works with same collection joins.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit works with same collection joins.")
    @Test
    void testComplexQueryWithSameCollectionJoin() throws SQLException {
        final String collection = "testComplexQueryJoin";
        final BsonDocument document1 =
                BsonDocument.parse("{ \"_id\" : \"key0\", \"field\": 0, \"array\": [1, 2, 3, 4, 5] }");
        final BsonDocument document2 =
                BsonDocument.parse("{ \"_id\" : \"key1\", \"field\": 0, \"array\": [1, 2, 3] }");
        final BsonDocument document3 =
                BsonDocument.parse("{ \"_id\" : \"key2\", \"field\": 0, \"array\": [1, 2] }");
        final BsonDocument document4 =
                BsonDocument.parse("{ \"_id\" : \"key3\", \"field\": 1, \"array\": [1, 2, 3, 4, 5] }");
        insertBsonDocuments(
                collection, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document1, document2, document3, document4});
        final Statement statement = getDocumentDbStatement();

        // Verify that result set has correct values.
        statement.execute(String.format(
                "SELECT SUM(\"%s\") as \"Sum\", COUNT(*) AS \"Count\" FROM \"%s\".\"%s\""
                        + "INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\""
                        + "WHERE \"%s\" <> 1 "
                        + "GROUP BY \"%s\".\"%s\" HAVING COUNT(*) > 1"
                        + "ORDER BY \"Count\" DESC LIMIT 1",
                "field",
                DATABASE_NAME, collection,
                DATABASE_NAME, collection + "_array",
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

    /**
     * Tests that different join types produce the correct result for tables from same collection.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests that different join types produce the correct result for tables from same collection.")
    @Test
    void testJoinTypesForTablesFromSameCollection() throws SQLException {
        final String collection = "testSameCollectionJoin";
        final BsonDocument document1 = BsonDocument.parse("{ \"_id\" : \"key0\", \"doc1\": { \"field\" : 1 } }");
        final BsonDocument document2 = BsonDocument.parse("{ \"_id\" : \"key1\", \"doc2\": { \"field\": 2 } }");
        insertBsonDocuments(
                collection, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document1, document2});
        final Statement statement = getDocumentDbStatement();

        // Verify that an inner join will return an empty result set.
        statement.execute(String.format(
                "SELECT * FROM \"%s\".\"%s\" INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                DATABASE_NAME, collection + "_doc1",
                DATABASE_NAME, collection + "_doc2",
                collection + "_doc1", collection + "__id",
                collection + "_doc2", collection + "__id"));
        final ResultSet resultSet1 = statement.getResultSet();
        Assertions.assertNotNull(resultSet1);
        Assertions.assertFalse(resultSet1.next());

        // Verify that a left outer join will return 1 row.
        statement.execute(String.format(
                "SELECT * FROM \"%s\".\"%s\" LEFT JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                DATABASE_NAME, collection + "_doc1",
                DATABASE_NAME, collection + "_doc2",
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

    @Disabled("Incorrect behaviour for right or full joins involving more than 2 virtual tables.")
    @DisplayName("Tests behaviour of right join for tables from the same collection.")
    @Test
    void testRightJoinForTablesFromSameCollection() throws SQLException {
        final String collection = "testSameCollectionRightJoin";
        final BsonDocument document1 =
                BsonDocument.parse(
                        "{ \"_id\" : \"key0\", \"doc1\": { \"field\" : 1 }, \"doc2\": { \"field\": 2 }}");
        final BsonDocument document2 = BsonDocument.parse("{ \"_id\" : \"key1\", \"doc2\": { \"field\": 2 } }");
        insertBsonDocuments(
                collection, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document1, document2});
        final Statement statement = getDocumentDbStatement();

        // Verify that a right outer join will return 1 rows.
        statement.execute(String.format(
                "SELECT * FROM \"%s\".\"%s\" RIGHT JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                DATABASE_NAME, collection,
                DATABASE_NAME, collection + "_doc1",
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
                        DATABASE_NAME,
                        collection,
                        DATABASE_NAME,
                        collection + "_doc1",
                        collection,
                        collection + "__id",
                        collection + "_doc1",
                        collection + "__id",
                        DATABASE_NAME,
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

    /**
     * Tests that a statement with project, where, group by, having, order, and limit works with same collection joins.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit works with a different collection join.")
    @Test
    void testComplexQueryWithDifferentCollectionJoin() throws SQLException {
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
                collection1, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document1, document2, document3, document4});
        insertBsonDocuments(
                collection2, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document5, document6, document7});
        final Statement statement = getDocumentDbStatement();

        // Verify that result set has correct values. Expecting query to single out document3.
        statement.execute(String.format(
                "SELECT \"%s\", SUM(\"%s\") as \"Sum\", COUNT(*) AS \"Count\" FROM \"%s\".\"%s\""
                        + "INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\""
                        + "WHERE \"%s\" <> 1 "
                        + "GROUP BY \"%s\".\"%s\" HAVING COUNT(*) < 5"
                        + "ORDER BY \"Count\" DESC LIMIT 1",
                collection2 + "__id",
                "field",
                DATABASE_NAME, collection1 + "_array",
                DATABASE_NAME, collection2,
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

    /**
     * "Tests that different join types produce the correct result for tables from different collections.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @DisplayName("Tests that different join types produce the correct result for tables from different collections.")
    @Test
    void testJoinTypesForTablesFromDifferentCollection() throws SQLException {
        final String collection1 = "testDifferentCollectionJoin1";
        final String collection2 = "testDifferentCollectionJoin2";
        final BsonDocument document1 =
                BsonDocument.parse(
                        "{ \"_id\" : \"key0\", \"array\": [ {\"field\": 1, \"field2\": \"value\"}, {\"field\": 2, \"field2\": \"value2\"}] }");
        final BsonDocument document2 = BsonDocument.parse(
                "{ \"_id\" : \"key1\", \"doc\": { \"field\": 1, field3: \"value3\"} }");
        insertBsonDocuments(
                collection1, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document1});
        insertBsonDocuments(
                collection2, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document2});
        final Statement statement = getDocumentDbStatement();

        // Verify that an inner join will return 1 row where field0 = field.
        statement.execute(String.format(
                "SELECT * FROM \"%s\".\"%s\" INNER JOIN \"%s\".\"%s\" ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                DATABASE_NAME, collection1 + "_array",
                DATABASE_NAME, collection2 + "_doc",
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
                DATABASE_NAME, collection1 + "_array",
                DATABASE_NAME, collection2 + "_doc",
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

    /**
     * Test querying using projection a three-level document.
     *
     * @throws SQLException occurs if executing the statement or retrieving a value fails.
     */
    @Test
    @DisplayName("Tests querying with projects on a three-level document. Addresses AD-115.")
    void testProjectionQueryWithThreeLevelDocument() throws SQLException {
        final String tableName = "testProjectionQueryWithThreeLevelDocument";
        final String keyColumnName = tableName + "__id";
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"doc\" : { \"field\" : 1, \"doc2\" : { \"field2\" : \"value\" } } }");
        insertBsonDocuments(tableName, DATABASE_NAME, USER, PASSWORD, new BsonDocument[]{document});
        final DocumentDbStatement statement = getDocumentDbStatement();

        // Verify the nested table from the field doc2 from the field doc.
        final ResultSet resultSet2 =
                statement.executeQuery(
                        String.format(
                                "SELECT \"%s__id\" FROM \"%s\".\"%s\"",
                                tableName, DATABASE_NAME, tableName + "_doc"));
        Assertions.assertNotNull(resultSet2);
        Assertions.assertTrue(resultSet2.next());
        Assertions.assertEquals("key", resultSet2.getString(keyColumnName));

        // Verify the nested table from the field doc2 from the field doc.
        final ResultSet resultSet3 =
                statement.executeQuery(
                        String.format(
                                "SELECT \"%s__id\" FROM \"%s\".\"%s\"",
                                tableName, DATABASE_NAME, tableName + "_doc_doc2"));
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
                                DATABASE_NAME,
                                tableName,
                                DATABASE_NAME,
                                tableName + "_doc",
                                tableName,
                                keyColumnName,
                                tableName + "_doc",
                                keyColumnName,
                                DATABASE_NAME,
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
