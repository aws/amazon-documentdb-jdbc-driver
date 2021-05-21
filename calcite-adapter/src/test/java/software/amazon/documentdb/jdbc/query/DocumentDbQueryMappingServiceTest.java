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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaException;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.sql.SQLException;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryMappingServiceTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String COLLECTION_NAME = "testCollection";
    private static final String OTHER_COLLECTION_NAME = "otherTestCollection";
    private static DocumentDbQueryMappingService queryMapper;
    private static DocumentDbConnectionProperties connectionProperties;

    @BeforeAll
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only")
    static void initialize() throws SQLException, DocumentDbSchemaException {
        // Add a valid users to the local MongoDB instance.
        connectionProperties = new DocumentDbConnectionProperties();
        createUser(DATABASE_NAME, "user", "password");
        connectionProperties.setUser("user");
        connectionProperties.setPassword("password");
        connectionProperties.setDatabase(DATABASE_NAME);
        connectionProperties.setTlsEnabled("false");
        connectionProperties.setHostname("localhost:" + getMongoPort());
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");

        final BsonDocument otherDocument =
                BsonDocument.parse(
                        "{ \"_id\" : \"key1\", \"otherArray\" : [ { \"field\" : 1, \"field3\": \"value\" }, { \"field\" : 2, \"field3\" : \"value\" } ]}");
        insertBsonDocuments(
                COLLECTION_NAME, DATABASE_NAME, "user", "password", new BsonDocument[] {document});
        insertBsonDocuments(
                OTHER_COLLECTION_NAME, DATABASE_NAME, "user", "password", new BsonDocument[] {otherDocument});
        final DocumentDbDatabaseSchemaMetadata databaseMetadata =
                DocumentDbDatabaseSchemaMetadata.get("id", connectionProperties, true);
        queryMapper = new DocumentDbQueryMappingService(connectionProperties, databaseMetadata);
    }

    @AfterAll
    static void afterAll() throws SQLException {
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(connectionProperties);
        schemaWriter.remove("id");
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
    }

    @Test
    @DisplayName("Tests that distinct keyword works when querying a single table.")
    void testQueryWithDistinct() throws SQLException {
        final String query =
                String.format(
                        "SELECT DISTINCT \"%s\" FROM \"%s\".\"%s\"",
                        "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$group\": {\"_id\": \"$array.field\"}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {\"array.field\": \"$_id\"}}"),
                result.getAggregateOperations().get(3));
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
        Assertions.assertEquals(2, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}, "
                        + "{\"array.field\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));

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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$sum\": {\"$cond\": [{\"$ifNull\": [\"$array.field1\", false]}, 1, 0]}}}}"),
                result.getAggregateOperations().get(2));

        final String queryWithDistinctCount =
                String.format(
                        "SELECT COUNT(DISTINCT \"%s\") FROM \"%s\".\"%s\"",
                        "field1", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(queryWithDistinctCount);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$addToSet\": \"$array.field1\"}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {\"_id\": 0, \"EXPR$0\": {\"$size\": \"$EXPR$0\"}}}"),
                result.getAggregateOperations().get(3));

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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$avg\": \"$array.field\"}}}"),
                result.getAggregateOperations().get(2));

        final String queryWithAverageDistinct =
                String.format(
                        "SELECT AVG(DISTINCT \"%s\") FROM \"%s\".\"%s\"",
                        "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(queryWithAverageDistinct);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$addToSet\": \"$array.field\"}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {\"_id\": 0, \"EXPR$0\": {\"$avg\": \"$EXPR$0\"}}}"),
                result.getAggregateOperations().get(3));

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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$sum\": \"$array.field\"}}}"),
                result.getAggregateOperations().get(2));

        final String queryWithSumDistinct =
                String.format(
                        "SELECT SUM(DISTINCT \"%s\") FROM \"%s\".\"%s\"",
                        "field", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(queryWithSumDistinct);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$addToSet\": \"$array.field\"}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {\"_id\": 0, \"EXPR$0\": {\"$sum\": \"$EXPR$0\"}}}"),
                result.getAggregateOperations().get(3));


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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"_0\": {\"$sum\": \"$array.field\"}, \"_1\": {\"$sum\": {\"$cond\": [{\"$ifNull\": [\"$array.field\", false]}, 1, 0]}}}}"),
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
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
        Assertions.assertEquals(8, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}, "
                        + "{\"array.field\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
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
    }

    @Test
    @DisplayName("Tests that a statement with join works for two tables from the same collection.")
    void testSameCollectionJoin() throws SQLException {
        final String innerJoin =
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
        final DocumentDbMqlQueryContext innerJoinResult = queryMapper.get(innerJoin);
        Assertions.assertNotNull(innerJoinResult);
        Assertions.assertEquals(COLLECTION_NAME, innerJoinResult.getCollectionName());
        Assertions.assertEquals(6, innerJoinResult.getColumnMetaData().size());
        Assertions.assertEquals(3, innerJoinResult.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                innerJoinResult.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                innerJoinResult.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {\"testCollection__id0\": {\"$cond\": [{\"$or\": [{\"$ifNull\": [\"$array.field\", false]}, {\"$ifNull\": [\"$array.field1\", false]}, {\"$ifNull\": [\"$array.field2\", false]}]}, \"$_id\", null]}, \"_id\": \"$_id\"}}"),
                innerJoinResult.getAggregateOperations().get(2));

        final String leftJoin =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + "LEFT JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME,
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME,
                        COLLECTION_NAME + "__id",
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "__id");
        final DocumentDbMqlQueryContext leftJoinResult = queryMapper.get(leftJoin);
        Assertions.assertNotNull(leftJoinResult);
        Assertions.assertEquals(COLLECTION_NAME, leftJoinResult.getCollectionName());
        Assertions.assertEquals(6, leftJoinResult.getColumnMetaData().size());
        Assertions.assertEquals(2, leftJoinResult.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                leftJoinResult.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {\"testCollection__id0\": {\"$cond\": [{\"$or\": [{\"$ifNull\": [\"$array.field\", false]}, {\"$ifNull\": [\"$array.field1\", false]}, {\"$ifNull\": [\"$array.field2\", false]}]}, \"$_id\", null]}, \"_id\": \"$_id\"}}"),
                leftJoinResult.getAggregateOperations().get(1));
    }

    @Test
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit "
            + "works for tables from the same collection.")
    void testComplexQueryWithSameCollectionJoin() throws SQLException {
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
        Assertions.assertEquals(9, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"array.field\": {\"$gt\": 1}}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$addFields\": "
                        + "{\"testCollection__id0\": {\"$cond\": [{\"$or\": ["
                        + "{\"$ifNull\": [\"$array.field\", false]}, "
                        + "{\"$ifNull\": [\"$array.field1\", false]}, "
                        + "{\"$ifNull\": [\"$array.field2\", false]}]}, \"$_id\", null]}, "
                        + "\"_id\": \"$_id\"}}"),
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
                BsonDocument.parse("{\"$limit\": 1}"),
                result.getAggregateOperations().get(8));
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

    @Test
    @DisplayName("Tests a simple query with a join between 2 different collections.")
    void testDifferentCollectionJoin() throws SQLException {
        final String innerJoin =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        DATABASE_NAME,
                        OTHER_COLLECTION_NAME + "_otherArray",
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "__id",
                        OTHER_COLLECTION_NAME + "_otherArray",
                        OTHER_COLLECTION_NAME + "__id");
        DocumentDbMqlQueryContext result = queryMapper.get(innerJoin);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(9, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": "
                                + "[{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}] }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$lookup\": "
                                + "{\"from\": \"otherTestCollection\", "
                                + "\"let\": "
                                + "{\"field1\": \"$array.field1\", \"field\": \"$array.field\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", \"field2\": \"$array.field2\", \"testCollection__id\": \"$_id\"}, "
                                + "\"pipeline\": ["
                                + "{\"$match\": {\"$or\": ["
                                    + "{\"otherArray.field\": {\"$exists\": true}}, "
                                    + "{\"otherArray.field3\": {\"$exists\": true}}]}}, "
                                + "{\"$unwind\": {\"path\": \"$otherArray\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"otherArray_index_lvl_0\"}}, "
                                + "{\"$match\": {\"$expr\": {\"$eq\": [\"$$testCollection__id\", \"$_id\"]}}}], "
                                + "\"as\": \"otherTestCollection_otherArray\"}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$otherTestCollection_otherArray\", \"preserveNullAndEmptyArrays\": false}}"),
                result.getAggregateOperations().get(3));

        final String leftJoin =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + "LEFT JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        DATABASE_NAME,
                        OTHER_COLLECTION_NAME + "_otherArray",
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "__id",
                        OTHER_COLLECTION_NAME + "_otherArray",
                        OTHER_COLLECTION_NAME + "__id");
        result = queryMapper.get(leftJoin);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(9, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": "
                                + "[{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}] }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$lookup\": "
                                + "{\"from\": \"otherTestCollection\", "
                                + "\"let\": "
                                + "{\"field1\": \"$array.field1\", \"field\": \"$array.field\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", \"field2\": \"$array.field2\", \"testCollection__id\": \"$_id\"}, "
                                + "\"pipeline\": ["
                                + "{\"$match\": {\"$or\": ["
                                        + "{\"otherArray.field\": {\"$exists\": true}}, "
                                        + "{\"otherArray.field3\": {\"$exists\": true}}]}}, "
                                + "{\"$unwind\": {\"path\": \"$otherArray\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"otherArray_index_lvl_0\"}}, "
                                + "{\"$match\": {\"$expr\": {\"$eq\": [\"$$testCollection__id\", \"$_id\"]}}}], "
                                + "\"as\": \"otherTestCollection_otherArray\"}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$otherTestCollection_otherArray\", \"preserveNullAndEmptyArrays\": true}}"),
                result.getAggregateOperations().get(3));
    }

    @Test
    @DisplayName("Tests that a statement with project, where, group by, having, order, and limit "
            + "works for tables from different collections.")
    void testComplexQueryWithDifferentCollectionJoin() throws SQLException {
        final String complexQuery =
            String.format(
                "SELECT \"%s\" AS \"renamed\", COUNT(*) AS \"Total\" FROM \"%s\".\"%s\""
                    + "INNER JOIN \"%s\".\"%s\""
                    + "ON \"%s\".\"%s\" = \"%s\".\"%s\""
                    + "WHERE \"%s\" > 1 GROUP BY \"%s\".\"%s\", \"%s\", \"%s\""
                    + "HAVING COUNT(*) > 1 ORDER BY \"renamed\" LIMIT 1",
                "field",
                DATABASE_NAME,
                OTHER_COLLECTION_NAME,
                DATABASE_NAME,
                COLLECTION_NAME + "_array",
                OTHER_COLLECTION_NAME,
                OTHER_COLLECTION_NAME + "__id",
                COLLECTION_NAME + "_array",
                COLLECTION_NAME + "__id",
                "field",
                OTHER_COLLECTION_NAME,
                OTHER_COLLECTION_NAME + "__id",
                "field",
                "field1");
        final DocumentDbMqlQueryContext result = queryMapper.get(complexQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(OTHER_COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(2, result.getColumnMetaData().size());
        Assertions.assertEquals(7, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$lookup\": "
                                + "{\"from\": \"testCollection\", "
                                + "\"let\": {\"otherTestCollection__id\": \"$_id\"}, "
                                + "\"pipeline\": ["
                                + "{\"$match\": {\"$or\": ["
                                    + "{\"array.field\": {\"$exists\": true}}, "
                                    + "{\"array.field1\": {\"$exists\": true}}, "
                                    + "{\"array.field2\": {\"$exists\": true}}]}}, "
                                + "{\"$unwind\": {"
                                    + "\"path\": \"$array\", "
                                    + "\"preserveNullAndEmptyArrays\": true, "
                                    + "\"includeArrayIndex\": \"array_index_lvl_0\"}}, "
                                + "{\"$match\": {\"array.field\": {\"$gt\": 1}}}, "
                                + "{\"$match\": {\"$expr\": {\"$eq\": [\"$$otherTestCollection__id\", \"$_id\"]}}}], "
                                + "\"as\": \"testCollection_array\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                 BsonDocument.parse("{\"$unwind\": {\"path\": \"$testCollection_array\", \"preserveNullAndEmptyArrays\": false}}"),
        result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {\"_id\": \"$_id\", "
                                + "\"testCollection_array_array_field\": \"$testCollection_array.array.field\", "
                                + "\"testCollection_array_array_field1\": \"$testCollection_array.array.field1\"}, "
                                + "\"Total\": {\"$sum\": 1}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": \"$_id._id\", "
                                + "\"testCollection_array.array.field\": \"$_id.testCollection_array_array_field\", "
                                + "\"testCollection_array.array.field1\": \"$_id.testCollection_array_array_field1\", \"Total\": \"$Total\"}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"Total\": {\"$gt\": 1}}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$sort\": {\"testCollection_array.array.field\": 1}}"),
                result.getAggregateOperations().get(5));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 1}"), result.getAggregateOperations().get(6));
    }

    @Test
    @DisplayName("Tests that unsupported join conditions or types throw an exception.")
    void testUnsupportedJoins() {
        // Cannot do right join on tables from different collections.
        final String rightJoinQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + "RIGHT JOIN \"%s\".\"%s\""
                                + "ON \"%s\" = \"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        DATABASE_NAME,
                        OTHER_COLLECTION_NAME + "_otherArray",
                        COLLECTION_NAME + "__id",
                        OTHER_COLLECTION_NAME + "__id");
        Assertions.assertEquals(
                String.format("Unable to parse SQL '%s'.", rightJoinQuery),
                Assertions.assertThrows(SQLException.class, () -> queryMapper.get(rightJoinQuery))
                        .getMessage());

        // Cannot do a full outer join on tables from different collections.
        final String fullJoinQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + "FULL JOIN \"%s\".\"%s\""
                                + "ON \"%s\" = \"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        DATABASE_NAME,
                        OTHER_COLLECTION_NAME + "_otherArray",
                        COLLECTION_NAME + "__id",
                        OTHER_COLLECTION_NAME + "__id");
        Assertions.assertEquals(
                String.format("Unable to parse SQL '%s'.", fullJoinQuery),
                Assertions.assertThrows(SQLException.class, () -> queryMapper.get(fullJoinQuery))
                        .getMessage());

        // Can only have a single equi-condition for a join between tables from same collection.
        final String multipleConditionsQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\""
                                + "ON \"%s\" = \"%s\""
                                + "OR \"%s\" > \"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        DATABASE_NAME,
                        OTHER_COLLECTION_NAME + "_otherArray",
                        COLLECTION_NAME + "__id",
                        OTHER_COLLECTION_NAME + "__id",
                        COLLECTION_NAME + "__id",
                        OTHER_COLLECTION_NAME + "__id");
        Assertions.assertEquals(
                String.format("Unable to parse SQL '%s'.", multipleConditionsQuery),
                Assertions.assertThrows(SQLException.class, () -> queryMapper.get(multipleConditionsQuery))
                        .getMessage());

        // Can only join tables from same collection on foreign keys.
        final String nonForeignKeyQuery =
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
                        "field");
        Assertions.assertEquals(
                String.format("Unable to parse SQL '%s'.", nonForeignKeyQuery),
                Assertions.assertThrows(SQLException.class, () -> queryMapper.get(nonForeignKeyQuery))
                        .getMessage());


        // Can only join tables from same collection on foreign keys.
        final String nonEqualityQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" > \"%s\".\"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME,
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME,
                        COLLECTION_NAME + "__id",
                        COLLECTION_NAME + "_array",
                        "field");
        Assertions.assertEquals(
                String.format("Unable to parse SQL '%s'.", nonEqualityQuery),
                Assertions.assertThrows(SQLException.class, () -> queryMapper.get(nonEqualityQuery))
                        .getMessage());
    }
}
