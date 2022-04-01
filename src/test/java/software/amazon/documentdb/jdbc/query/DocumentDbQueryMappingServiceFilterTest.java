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
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbFilter;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;

import java.sql.SQLException;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryMappingServiceFilterTest extends DocumentDbQueryMappingServiceTest {
    private static final String COLLECTION_NAME = "testCollection";
    private static DocumentDbQueryMappingService queryMapper;

    @BeforeAll
    void initialize() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", "
                                + "\"document\": { \"booleanField\": true, \"booleanField2\": false} "
                                + "\"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        insertBsonDocuments(COLLECTION_NAME, new BsonDocument[]{document});
        queryMapper = getQueryMappingService();
    }

    @Test
    @DisplayName("Test queries with WHERE f1 IN (c1, c2...)")
    void testQueryWithIn() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" IN (2, 3)" , getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$eq\": 2}}, {\"array.field\": {\"$eq\": 3}}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(3));
    }

    @Test
    @DisplayName("Test queries with WHERE f1 NOT IN (c1, c2...)")
    void testQueryWithNotIn() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" NOT IN (2, 3)" , getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$and\": [{\"array.field\": {\"$nin\": [null, 2]}}, {\"array.field\": {\"$nin\": [null, 3]}}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(3));
    }

    @Test
    @DisplayName("Tests queries with IS [NOT] NULL")
    void testQueryIsNull() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" IS NULL OR \"field1\" IS NOT NULL" , getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$eq\": null }},{\"array.field1\": {\"$ne\": null }}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(3));

    }

    @Test
    @DisplayName("Tests queries with where clause containing arithmetic.")
    void testQueryArithmeticWhere() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" * \"field1\" / \"field2\" + \"field1\" - \"field2\" = 7",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"_id\": 1, "
                                + "\"array_index_lvl_0\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, "
                                + "\"array.field2\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$and\": [{\"$gt\": [{\"$subtract\": [{\"$add\": [{\"$divide\": [{\"$multiply\": [\"$array.field\", \"$array.field1\"]}, \"$array.field2\"]}, \"$array.field1\"]}, \"$array.field2\"]}, null]}, "
                                + "{\"$gt\": [{\"$literal\": 7}, null]}]}, {\"$eq\": [{\"$subtract\": [{\"$add\": [{\"$divide\": [{\"$multiply\": [\"$array.field\", \"$array.field1\"]}, \"$array.field2\"]}, \"$array.field1\"]}, \"$array.field2\"]}, "
                                + "{\"$literal\": 7}]}, null]}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(5));
    }

    @Test
    @DisplayName("Tests queries with where clause containing modulo.")
    void testQueryModuloWhere() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE MOD(\"field\", 3) = 2" +
                                "OR MOD(8, \"field\") = 2" +
                                "OR MOD(3, 2) = \"field\"",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"_id\": 1, "
                                + "\"array_index_lvl_0\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, "
                                + "\"array.field2\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$or\": [{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [{\"$mod\": [\"$array.field\", {\"$literal\": 3}]}, null]}, "
                                + "{\"$gt\": [{\"$literal\": 2}, null]}]}, {\"$eq\": [{\"$mod\": [\"$array.field\", {\"$literal\": 3}]}, {\"$literal\": 2}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [{\"$mod\": [{\"$literal\": 8}, \"$array.field\"]}, null]}, {\"$gt\": [{\"$literal\": 2}, null]}]}, "
                                + "{\"$eq\": [{\"$mod\": [{\"$literal\": 8}, \"$array.field\"]}, {\"$literal\": 2}]}, null]}]}, {\"$eq\": [true, {\"$cond\": [{\"$and\": ["
                                + "{\"$gt\": [{\"$literal\": 1}, null]}, {\"$gt\": [\"$array.field\", null]}]}, "
                                + "{\"$eq\": [{\"$literal\": 1}, \"$array.field\"]}, null]}]}]}, true, {\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": ["
                                + "{\"$gt\": [{\"$mod\": [\"$array.field\", {\"$literal\": 3}]}, null]}, "
                                + "{\"$gt\": [{\"$literal\": 2}, null]}]}, {\"$eq\": [{\"$mod\": [\"$array.field\", {\"$literal\": 3}]}, {\"$literal\": 2}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [{\"$mod\": [{\"$literal\": 8}, \"$array.field\"]}, null]}, "
                                + "{\"$gt\": [{\"$literal\": 2}, null]}]}, "
                                + "{\"$eq\": [{\"$mod\": [{\"$literal\": 8}, \"$array.field\"]}, {\"$literal\": 2}]}, null]}]}, {\"$eq\": [false, {\"$cond\": [{\"$and\": ["
                                + "{\"$gt\": [{\"$literal\": 1}, null]}, {\"$gt\": [\"$array.field\", null]}]}, "
                                + "{\"$eq\": [{\"$literal\": 1}, \"$array.field\"]}, null]}]}]}, false, null]}]}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(5));

    }

    @Test
    @DisplayName("Tests queries with where clause containing nested OR.")
    void testQueryWhereNestedOr() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" > 0 OR (\"field1\" > 0 OR \"field2\" > 6)",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$gt\": 0}}, {\"array.field1\": {\"$gt\": 0}}, {\"array.field2\": {\"$gt\": 6}}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(3));
    }

    @Test
    @DisplayName("Tests queries with where clause containing nested AND.")
    void testQueryWhereNestedAnd() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" > 0 AND (\"field1\" > 0 AND \"field2\" > 6)",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$and\": [{\"array.field\": {\"$gt\": 0}}, {\"array.field1\": {\"$gt\": 0}}, {\"array.field2\": {\"$gt\": 6}}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(3));

    }

    @Test
    @DisplayName("Tests queries with where clause containing nested combined NOT, OR, and AND.")
    void testQueryWhereNotAndOr() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE ((NOT \"field\" > 0 AND \"field2\" < 10) AND (NOT \"field1\" > 0 OR \"field2\" > 6)) OR \"field2\" > 0",
                        getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"$and\": [{\"array.field\": {\"$lte\": 0}}, {\"array.field2\": {\"$lt\": 10}}, "
                                + "{\"$or\": [{\"array.field1\": {\"$lte\": 0}}, {\"array.field2\": {\"$gt\": 6}}]}]}, "
                                + "{\"array.field2\": {\"$gt\": 0}}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(3));
    }

    @Test
    @DisplayName("Tests queries with where clause comparing two columns.")
    void testQueryWhereTwoColumns() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" = \"field2\"" ,
                        getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"_id\": 1, "
                                + "\"array_index_lvl_0\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, "
                                + "\"array.field2\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [\"$array.field2\", null]}]}, "
                                + "{\"$eq\": [\"$array.field\", \"$array.field2\"]}, null]}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(5));
    }

    @Test
    @DisplayName("Tests that queries comparing the result of simple comparisons use the aggregate operator syntax.")
    void testQueryWhereNestedCompare() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE (\"field\" IS NULL) = (\"field2\" IS NULL)" ,
                        getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"_id\": 1, "
                                + "\"array_index_lvl_0\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, "
                                + "\"array.field2\": 1, "
                                + "\"placeholderField1F84EB1G3K47\""
                                + ": {\"$cond\": [{\"$and\": [{\"$gt\": [{\"$lte\": [\"$array.field\", null]}, null]}, "
                                + "{\"$gt\": [{\"$lte\": [\"$array.field2\", null]}, null]}]}, "
                                + "{\"$eq\": [{\"$lte\": [\"$array.field\", null]}, {\"$lte\": [\"$array.field2\", null]}]}, null]}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(5));
    }

    @Test
    @DisplayName("Tests queries with where clause using value of boolean columns.")
    void testQueryWhereBooleanFields() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"booleanField\" AND NOT \"booleanField2\"" ,
                        getDatabaseName(), COLLECTION_NAME + "_document");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(3, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"document.booleanField\": {\"$exists\": true}}, {\"document.booleanField2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$and\": [{\"document.booleanField\": true}, {\"document.booleanField2\": false}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"booleanField\": \"$document.booleanField\", "
                                + "\"booleanField2\": \"$document.booleanField2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
    }

    @Test
    @DisplayName("Tests queries with where clause combining literal comparison and field vs field comparison.")
    void testQueryWhereFieldAndSimpleComparison() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" < 2 OR \"field\" = \"field2\"" ,
                        getDatabaseName(), COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"_id\": 1, "
                                + "\"array_index_lvl_0\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, "
                                + "\"array.field2\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$or\": [{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [{\"$literal\": 2}, null]}]}, "
                                + "{\"$lt\": [\"$array.field\", {\"$literal\": 2}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [\"$array.field2\", null]}]}, "
                                + "{\"$eq\": [\"$array.field\", \"$array.field2\"]}, null]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [{\"$literal\": 2}, null]}]}, "
                                + "{\"$lt\": [\"$array.field\", {\"$literal\": 2}]}, null]}]}, {\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [\"$array.field2\", null]}]}, "
                                + "{\"$eq\": [\"$array.field\", \"$array.field2\"]}, null]}]}]}, false, null]}]}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(5));
    }
}
