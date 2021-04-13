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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import java.sql.SQLException;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryMappingServiceTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String COLLECTION_NAME = "testCollection";
    private DocumentDbQueryMappingService queryMapper;

    @BeforeAll
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only")
    void initialize() throws SQLException {
        // Add a valid users to the local MongoDB instance.
        final DocumentDbConnectionProperties connectionProperties =
                new DocumentDbConnectionProperties();
        createUser(DATABASE_NAME, "user", "password");
        connectionProperties.setUser("user");
        connectionProperties.setPassword("password");
        connectionProperties.setDatabase(DATABASE_NAME);
        connectionProperties.setTlsEnabled("false");
        connectionProperties.setHostname("localhost:" + getMongoPort());
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");

        insertBsonDocuments(
                COLLECTION_NAME, DATABASE_NAME, "user", "password", new BsonDocument[] {document});
        final DocumentDbDatabaseSchemaMetadata databaseMetadata =
                DocumentDbDatabaseSchemaMetadata.get("id", connectionProperties, true);
        queryMapper = new DocumentDbQueryMappingService(connectionProperties, databaseMetadata);
    }

    @Test
    @DisplayName("Tests that select works for querying single base or virtual tables.")
    void testQueryWithSelect() throws SQLException {
        // Get the base table.
        final String basicQuery =
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, COLLECTION_NAME);
        DocumentDbMqlQueryContext result = queryMapper.get(basicQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(0, result.getAggregateOperations().size());

        // Get the nested table.
        final String nestedTableQuery =
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(nestedTableQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(2, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Tests that project works for querying a single table.")
    void testQueryWithProject() throws SQLException {
        final String query =
                String.format(
                        "SELECT \"%s\" FROM \"%s\".\"%s\"", "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(2, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
    }

    @Test
    @DisplayName("Tests that as works when querying a single table.")
    void testQueryWithAs() throws SQLException {
        final String query =
                String.format(
                        "SELECT \"%s\" AS \"renamed\" FROM \"%s\".\"%s\"",
                        "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(" {\"$match\": {\"array\": {\"$exists\": true } } }"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$addFields\": {\"renamed\": \"$array.field\"} }"),
                result.getAggregateOperations().get(2));
    }

    @Test
    @DisplayName("Tests that where works with 1 or more conditions when querying a single table.")
    void testQueryWithWhere() throws SQLException {
        final String queryWithWhere =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"%s\" = %s",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field", 1);
        DocumentDbMqlQueryContext result = queryMapper.get(queryWithWhere);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$match\": {\"array.field\": 1} }"),
                result.getAggregateOperations().get(2));

        final String queryWithCompoundWhere =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"%s\" = '%s' AND \"%s\" > %s",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field1", "value", "field", 0);
        result = queryMapper.get(queryWithCompoundWhere);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array.field1\": \"value\", \"array.field\": { \"$gt\": 0 } } }"),
                result.getAggregateOperations().get(2));
    }

    @Test
    @DisplayName("Tests that limit works when querying a single table.")
    void testQueryWithLimit() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" LIMIT 1", DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 1}"), result.getAggregateOperations().get(2));
    }

    @Test
    @DisplayName("Tests that order by works with 1 or more sort conditions in ascending and descending order.")
    void testQueryWithOrderBy() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field");
        DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$sort\": {\"array.field\": 1 } }"),
                result.getAggregateOperations().get(2));

        final String queryWithDescendingSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" DESC",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field");
        result = queryMapper.get(queryWithDescendingSort);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$sort\": {\"array.field\": -1 } }"),
                result.getAggregateOperations().get(2));

        final String queryWithCompoundSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC, \"%s\" DESC",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field", "field1");
        result = queryMapper.get(queryWithCompoundSort);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{ \"$sort\": {\"array.field\": 1, \"array.field1\": -1 } }"),
                result.getAggregateOperations().get(2));
    }

    @Test
    @DisplayName("Tests that group by works when querying a single table.")
    void testQueryWithGroupBy() throws SQLException {
        final String queryWithGroupBy =
                String.format(
                        "SELECT \"%s\", \"%s\", \"%s\" FROM \"%s\".\"%s\" GROUP BY \"%s\", \"%s\", \"%s\"",
                        COLLECTION_NAME + "__id",
                        "field",
                        "field1",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "__id",
                        "field",
                        "field1");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithGroupBy);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(3, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {\"_id\": \"$_id\", \"array_field\": \"$array.field\", \"array_field1\": \"$array.field1\"}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": \"$_id._id\", \"array.field\": \"$_id.array_field\", \"array.field1\": \"$_id.array_field1\"}}"),
                result.getAggregateOperations().get(3));
    }

    @Test
    @DisplayName("Tests that count, sum, min, max, and avg work when querying a single table.")
    void testQueryWithAggregateFunctions() throws SQLException {
        final String queryWithCount =
                String.format(
                        "SELECT COUNT(\"%s\") FROM \"%s\".\"%s\"",
                        "field1", DATABASE_NAME, COLLECTION_NAME + "_array");
        DocumentDbMqlQueryContext result = queryMapper.get(queryWithCount);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$sum\": {\"$cond\": [{\"$eq\": [\"array.field1\", null]}, 0, 1]}}}}"),
                result.getAggregateOperations().get(2));

        final String queryWithAverage =
                String.format(
                        "SELECT AVG(\"%s\") FROM \"%s\".\"%s\"",
                        "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(queryWithAverage);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$avg\": \"$array.field\"}}}"),
                result.getAggregateOperations().get(2));

        final String queryWithSum =
                String.format(
                        "SELECT SUM(\"%s\") FROM \"%s\".\"%s\"",
                        "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(queryWithSum);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$sum\": \"$array.field\"}}}"),
                result.getAggregateOperations().get(2));

        final String queryWithMin =
                String.format(
                        "SELECT MIN(\"%s\") FROM \"%s\".\"%s\"",
                        "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(queryWithMin);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$min\": \"$array.field\"}}}"),
                result.getAggregateOperations().get(2));

        final String queryWithMax =
                String.format(
                        "SELECT MAX(\"%s\") FROM \"%s\".\"%s\"",
                        "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(queryWithMax);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$max\": \"$array.field\"}}}"),
                result.getAggregateOperations().get(2));
    }

    @Test
    @DisplayName("Tests that arithmetic operators work when querying a single table.")
    void testQueryWithArithmeticOperators() throws SQLException {
        final String queryWithSum =
                String.format(
                        "SELECT SUM(\"%s\") / COUNT(\"%s\") FROM \"%s\".\"%s\"",
                        "field", "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithSum);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"_0\": {\"$sum\": \"$array.field\"}, \"_1\": {\"$sum\": {\"$cond\": [{\"$eq\": [\"array.field\", null]}, 0, 1]}}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {\"EXPR$0\": {\"$divide\": [{\"$cond\": [{\"$eq\": [\"$_1\", {\"$literal\": 0}]}, null, \"$_0\"]}, \"$_1\"]}}}"),
                result.getAggregateOperations().get(3));
    }

    @Test
    @DisplayName("Tests that having works when querying a single table.")
    void testQueryWithHaving() throws SQLException {
        final String queryWithHaving =
                String.format(
                        "SELECT \"%s\", \"%s\", \"%s\" FROM \"%s\".\"%s\""
                                + "GROUP BY \"%s\", \"%s\", \"%s\" HAVING COUNT(*) > 1",
                        COLLECTION_NAME + "__id",
                        "field",
                        "field1",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "__id",
                        "field",
                        "field1");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithHaving);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(3, result.getColumnMetaData().size());
        Assertions.assertEquals(5, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {\"_id\": \"$_id\", \"array_field\": \"$array.field\", \"array_field1\": \"$array.field1\"}, \"_3\": {\"$sum\": 1}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": \"$_id._id\", \"array.field\": \"$_id.array_field\", \"array.field1\": \"$_id.array_field1\", \"_3\": \"$_3\"}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"_3\": {\"$gt\": 1}}}"),
                result.getAggregateOperations().get(4));
    }

    @Test
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit works for a single table.")
    void testComplexQuery() throws SQLException {
        final String complexQuery =
                String.format(
                        "SELECT \"%s\", \"%s\" AS \"renamed\", COUNT(*) AS \"Total\" FROM \"%s\".\"%s\""
                                + "WHERE \"%s\" = 'key' GROUP BY \"%s\", \"%s\", \"%s\""
                                + "HAVING COUNT(*) > 1 ORDER BY \"renamed\" LIMIT 1",
                        COLLECTION_NAME + "__id",
                        "field",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "__id",
                        COLLECTION_NAME + "__id",
                        "field",
                        "field1");
        final DocumentDbMqlQueryContext result = queryMapper.get(complexQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(3, result.getColumnMetaData().size());
        Assertions.assertEquals(9, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"_id\": \"key\"}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {\"_id\": \"$_id\", \"array_field\": \"$array.field\", \"array_field1\": \"$array.field1\"}, \"Total\": {\"$sum\": 1}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": \"$_id._id\", \"array.field\": \"$_id.array_field\", \"array.field1\": \"$_id.array_field1\", \"Total\": \"$Total\"}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"Total\": {\"$gt\": 1}}}"),
                result.getAggregateOperations().get(5));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$sort\": {\"array.field\": 1}}"),
                result.getAggregateOperations().get(6));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 1}"), result.getAggregateOperations().get(7));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$addFields\": {\"renamed\": \"$array.field\"}}"),
                result.getAggregateOperations().get(8));
    }

    @Test
    @DisplayName("Tests that a statement with join works for two tables from the same collection.")
    void testSimpleJoin() throws SQLException {
        final String complexQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME,
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME,
                        COLLECTION_NAME + "__id",
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "__id");
        final DocumentDbMqlQueryContext result = queryMapper.get(complexQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(6, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$match\": {\"array\": {\"$exists\": true } } }"), result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {\"_id\": \"$_id\", \"testCollection__id0\": {\"$cond\": [{\"$ifNull\": [\"$array\", false]}, \"$_id\", null]}}}"),
                result.getAggregateOperations().get(2));
    }
    @Test
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit "
            + "works for tables from the same collection.")
    void testComplexQueryWithJoin() throws SQLException {
        final String complexQuery =
                String.format(
                        "SELECT \"%s\" AS \"renamed\", COUNT(*) AS \"Total\" FROM \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\""
                                + "WHERE \"%s\" > 1 GROUP BY \"%s\".\"%s\", \"%s\", \"%s\""
                                + "HAVING COUNT(*) > 1 ORDER BY \"renamed\" LIMIT 1",
                        "field",
                        DATABASE_NAME,
                        COLLECTION_NAME,
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME,
                        COLLECTION_NAME + "__id",
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "__id",
                        "field",
                        COLLECTION_NAME,
                        COLLECTION_NAME + "__id",
                        "field",
                        "field1");
        final DocumentDbMqlQueryContext result = queryMapper.get(complexQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(2, result.getColumnMetaData().size());
        Assertions.assertEquals(10, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {\"path\": \"$array\", \"includeArrayIndex\" : \"array_index_lvl_0\"} }"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"array.field\": {\"$gt\": 1}}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"array\": {\"$exists\": true}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                "{\"$addFields\": {\"testCollection__id0\": {\"$cond\": [{\"$ifNull\": [\"$array\", false]}, \"$_id\", null]}, \"_id\": \"$_id\"}}"),
        result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {\"_id\": \"$_id\", \"array_field\": \"$array.field\", \"array_field1\": \"$array.field1\"}, \"Total\": {\"$sum\": 1}}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": \"$_id._id\", \"array.field\": \"$_id.array_field\", \"array.field1\": \"$_id.array_field1\", \"Total\": \"$Total\"}}"),
                result.getAggregateOperations().get(5));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"Total\": {\"$gt\": 1}}}"),
                result.getAggregateOperations().get(6));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$sort\": {\"array.field\": 1}}"),
                result.getAggregateOperations().get(7));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 1}"), result.getAggregateOperations().get(8));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$addFields\": {\"renamed\": \"$array.field\"}}"),
                result.getAggregateOperations().get(9));
    }

    @Test
    @DisplayName("Tests that a valid query that cannot be executed purely with aggregate throws an exception.")
    void testUnsupportedQuery() {
        // Union requires 2 separate calls.
        final String query =
                String.format("SELECT * FROM \"%s\".\"%s\" UNION SELECT \"%s\" FROM \"%s\".\"%s\"",
                        DATABASE_NAME, COLLECTION_NAME, COLLECTION_NAME + "__id", DATABASE_NAME, COLLECTION_NAME + "_array");
        Assertions.assertEquals(String.format("Unsupported SQL syntax '%s'.", query),
                Assertions.assertThrows(SQLException.class, () -> queryMapper.get(query))
                        .getMessage());
    }

    @Test
    @DisplayName("Tests that an invalid query throws an exception.")
    void testInvalidQuery() {
        // Column counts here are mismatched so this is invalid sql.
        final String query =
                String.format("SELECT * FROM \"%s\".\"%s\" UNION SELECT * FROM \"%s\".\"%s\"",
                        DATABASE_NAME, COLLECTION_NAME, DATABASE_NAME, COLLECTION_NAME + "_array");
        Assertions.assertEquals(String.format("Unable to parse SQL '%s'.", query),
                Assertions.assertThrows(SQLException.class, () -> queryMapper.get(query))
                        .getMessage());
    }
}
