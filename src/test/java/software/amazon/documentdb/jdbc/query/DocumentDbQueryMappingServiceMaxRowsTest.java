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
public class DocumentDbQueryMappingServiceMaxRowsTest extends DocumentDbQueryMappingServiceTest {
    private static final String COLLECTION_NAME = "testCollection";
    private static DocumentDbQueryMappingService queryMapper;

    @BeforeAll
    void initialize() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        insertBsonDocuments(COLLECTION_NAME, new BsonDocument[]{document});
        queryMapper = getQueryMappingService();
    }

    @Test
    @DisplayName("Tests $limit is produced when max rows is passed.")
    void testMaxRows() throws SQLException {
        final String query =
                String.format("SELECT * FROM \"%s\".\"%s\"", getDatabaseName(), COLLECTION_NAME);
        final DocumentDbMqlQueryContext result = queryMapper.get(query, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(2, result.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"testCollection__id\": '$_id', \"_id\": 0}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"10\"}}"), result.getAggregateOperations().get(1));
    }

    @Test
    @DisplayName("Tests $limit and $sort are produced when max rows is set.")
    void testOrderByWithMaxRows() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC",
                        getDatabaseName(), COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(5, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$sort\": {\"field\": 1 } }"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"10\"}}"), result.getAggregateOperations().get(4));
    }

    @Test
    @DisplayName("Tests $limit and $sort are produced when max rows is set and the query has a limit.")
    void testOrderByWithMaxRowsAndLimit() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC LIMIT 5",
                        getDatabaseName(), COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$sort\": {\"field\": 1 } }"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"5\"}}"), result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"10\"}}"), result.getAggregateOperations().get(5));
    }

    @Test
    @DisplayName("Tests $limit and $sort are produced when max rows is set and the subquery has a limit.")
    void testOrderByWithMaxRowsAndInnerLimit() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM ( SELECT * FROM \"%s\".\"%s\" LIMIT 20 ) ORDER BY \"%s\" ASC",
                        getDatabaseName(), COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(7, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"20\"}}}}"), result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$testCollection__id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$field\", "
                                + "\"field1\": \"$field1\", "
                                + "\"field2\": \"$field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$sort\": {\"field\": 1 } }"),
                result.getAggregateOperations().get(5));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"10\"}}"), result.getAggregateOperations().get(6));
    }

    @Test
    @DisplayName("Tests $limit and $sort are produced when max rows is set and the sub query and the query has limit.")
    void testOrderByWithMaxRowsAndInnerLimitAndOuterLimit() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM  (SELECT * FROM \"%s\".\"%s\" LIMIT 20)  ORDER BY \"%s\" ASC LIMIT 30",
                        getDatabaseName(), COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(8, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"20\"}}}"), result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$testCollection__id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$field\", "
                                + "\"field1\": \"$field1\", "
                                + "\"field2\": \"$field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$sort\": {\"field\": 1 } }"),
                result.getAggregateOperations().get(5));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"30\"}}}"), result.getAggregateOperations().get(6));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"10\"}}"), result.getAggregateOperations().get(7));
    }

    @Test
    @DisplayName("Tests $limit is produced and $sort is omitted when max rows is set and the sub query and the query has  no limit.")
    void testOrderByWithMaxRowsAndInnerOrderBy() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM  (SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC) ",
                        getDatabaseName(), COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"10\"}}"), result.getAggregateOperations().get(3));
    }
}
