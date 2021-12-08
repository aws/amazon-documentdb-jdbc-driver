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

package software.amazon.documentdb.jdbc.query;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;

import java.sql.SQLException;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryMappingServiceStringTest extends DocumentDbQueryMappingServiceTest {
    private static final String COLLECTION_NAME = "testCollection";
    private static DocumentDbQueryMappingService queryMapper;

    @BeforeAll
    void initialize() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse("{ \"_id\" : \"key\", \"field\": \"Hello, world!\"}");
        insertBsonDocuments(COLLECTION_NAME, new BsonDocument[] {document});
        queryMapper = getQueryMappingService();
    }

    @Test
    @DisplayName("Test queries with UPPER() and fn-escaped UCASE().")
    void testQueryWithUpper() throws SQLException {
        final String query1 =
                String.format(
                        "SELECT UPPER(\"field\") FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}]}, {\"$toUpper\": \"$field\"}, null]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        final String query2 =
                String.format(
                        "SELECT {fn UCASE(\"field\")} FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}]}, {\"$toUpper\": \"$field\"}, null]}, \"_id\": 0}}"),
                result2.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Test queries with LOWER() and fn-escaped LCASE().")
    void testQueryWithLower() throws SQLException {
        final String query1 =
                String.format(
                        "SELECT LOWER(\"field\") FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}]}, {\"$toLower\": \"$field\"}, null]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        final String query2 =
                String.format(
                        "SELECT {fn LCASE(\"field\")} FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}]}, {\"$toLower\": \"$field\"}, null]}, \"_id\": 0}}"),
                result2.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Test queries with CHAR_LENGTH(), CHARACTER_LENGTH() & fn-escaped LENGTH().")
    void testQueryWithCharLength() throws SQLException {
        final String query1 =
                String.format(
                        "SELECT CHAR_LENGTH(\"field\") FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}]}, {\"$strLenCP\": \"$field\"}, null]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        final String query2 =
                String.format(
                        "SELECT CHARACTER_LENGTH(\"field\") FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}]}, {\"$strLenCP\": \"$field\"}, null]}, \"_id\": 0}}"),
                result2.getAggregateOperations().get(0));

        final String query3 =
                String.format(
                        "SELECT {fn LENGTH(\"field\")} FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result3 = queryMapper.get(query3);
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(COLLECTION_NAME, result3.getCollectionName());
        Assertions.assertEquals(1, result3.getColumnMetaData().size());
        Assertions.assertEquals(1, result3.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}]}, {\"$strLenCP\": \"$field\"}, null]}, \"_id\": 0}}"),
                result3.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Test queries with string concatenation using ||, CONCAT() and fn-escaped CONCAT().")
    void testQueryWithStringConcatenation() throws SQLException {
        final String query1 =
                String.format(
                        "SELECT CONCAT(\"field\", '!!!') FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$concat\": [{\"$ifNull\": [\"$field\", \"\"]}, {\"$ifNull\": [{\"$literal\": \"!!!\"}, \"\"]}]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        final String query2 =
                String.format(
                        "SELECT \"field\" || '!!!' FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$concat\": [\"$field\", {\"$literal\": \"!!!\"}]}, \"_id\": 0}}"),
                result2.getAggregateOperations().get(0));

        final String query3 =
                String.format(
                        "SELECT {fn CONCAT(\"field\", '!!!')} FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result3 = queryMapper.get(query3);
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(COLLECTION_NAME, result3.getCollectionName());
        Assertions.assertEquals(1, result3.getColumnMetaData().size());
        Assertions.assertEquals(1, result3.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$concat\": [\"$field\", {\"$literal\": \"!!!\"}]}, \"_id\": 0}}"),
                result3.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Test queries with POSITION() & fn-escaped LOCATE() with 2 and 3 arguments")
    void testQueryWithPosition() throws SQLException {
        final String query1 =
                String.format(
                        "SELECT POSITION('world' IN \"field\") FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [{\"$literal\": \"world\"}, null]}, {\"$gt\": [\"$field\", null]}]}, {\"$add\": [{\"$indexOfCP\": [{\"$toLower\": \"$field\"}, {\"$toLower\": {\"$literal\": \"world\"}}]}, 1]}, null]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        final String query2 =
                String.format(
                        "SELECT POSITION('world' IN \"field\" FROM 3) FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [{\"$literal\": \"world\"}, null]}, {\"$gt\": [\"$field\", null]}, {\"$gt\": [{\"$literal\": 3}, null]}]}, {\"$cond\": [{\"$lte\": [{\"$literal\": 3}, 0]}, 0, {\"$add\": [{\"$indexOfCP\": [{\"$toLower\": \"$field\"}, {\"$toLower\": {\"$literal\": \"world\"}}, {\"$subtract\": [{\"$literal\": 3}, 1]}]}, 1]}]}, null]}, \"_id\": 0}}"),
                result2.getAggregateOperations().get(0));

        final String query3 =
                String.format(
                        "SELECT {fn LOCATE('world', \"field\", 3)} FROM \"%s\".\"%s\"",
                        getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result3 = queryMapper.get(query3);
        Assertions.assertNotNull(result3);
        Assertions.assertEquals(COLLECTION_NAME, result3.getCollectionName());
        Assertions.assertEquals(1, result3.getColumnMetaData().size());
        Assertions.assertEquals(1, result3.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [{\"$literal\": \"world\"}, null]}, {\"$gt\": [\"$field\", null]}, {\"$gt\": [{\"$literal\": 3}, null]}]}, {\"$cond\": [{\"$lte\": [{\"$literal\": 3}, 0]}, 0, {\"$add\": [{\"$indexOfCP\": [{\"$toLower\": \"$field\"}, {\"$toLower\": {\"$literal\": \"world\"}}, {\"$subtract\": [{\"$literal\": 3}, 1]}]}, 1]}]}, null]}, \"_id\": 0}}"),
                result3.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Test queries with LEFT() and fn-escaped LEFT().")
    void testQueryWithLeft() throws SQLException {
        final String query1 =
                String.format(
                        "SELECT LEFT(\"field\", 5) FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}, {\"$gt\": [{\"$literal\": 5}, null]}]}, {\"$substrCP\": [\"$field\", 0, {\"$literal\": 5}]}, null]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        final String query2 =
                String.format(
                        "SELECT {fn LEFT(\"field\", 5)} FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}, {\"$gt\": [{\"$literal\": 5}, null]}]}, {\"$substrCP\": [\"$field\", 0, {\"$literal\": 5}]}, null]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Test queries with RIGHT() and fn-escaped RIGHT().")
    void testQueryWithRight() throws SQLException {
        final String query1 =
                String.format(
                        "SELECT RIGHT(\"field\", 5) FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result1 = queryMapper.get(query1);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(COLLECTION_NAME, result1.getCollectionName());
        Assertions.assertEquals(1, result1.getColumnMetaData().size());
        Assertions.assertEquals(1, result1.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}, {\"$gt\": [{\"$literal\": 5}, null]}]}, {\"$cond\": [{\"$lte\": [{\"$strLenCP\": \"$field\"}, {\"$literal\": 5}]}, \"$field\", {\"$substrCP\": [\"$field\", {\"$subtract\": [{\"$strLenCP\": \"$field\"}, {\"$literal\": 5}]}, {\"$literal\": 5}]}]}, null]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));

        final String query2 =
                String.format(
                        "SELECT {fn RIGHT(\"field\", 5)} FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result2 = queryMapper.get(query2);
        Assertions.assertNotNull(result2);
        Assertions.assertEquals(COLLECTION_NAME, result2.getCollectionName());
        Assertions.assertEquals(1, result2.getColumnMetaData().size());
        Assertions.assertEquals(1, result2.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}, {\"$gt\": [{\"$literal\": 5}, null]}]}, {\"$cond\": [{\"$lte\": [{\"$strLenCP\": \"$field\"}, {\"$literal\": 5}]}, \"$field\", {\"$substrCP\": [\"$field\", {\"$subtract\": [{\"$strLenCP\": \"$field\"}, {\"$literal\": 5}]}, {\"$literal\": 5}]}]}, null]}, \"_id\": 0}}"),
                result1.getAggregateOperations().get(0));
    }
}
