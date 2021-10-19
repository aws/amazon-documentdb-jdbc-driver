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

import com.mongodb.client.MongoClient;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbFilter;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.sql.SQLException;
import java.time.Instant;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_NEW;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryMappingServiceTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String COLLECTION_NAME = "testCollection";
    private static final String OTHER_COLLECTION_NAME = "otherTestCollection";
    private static final String DATE_COLLECTION_NAME = "dateTestCollection";
    private static final String NESTED_ID_COLLECTION_NAME = "nestedIdCollection";
    private static final String NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME = "nestedArrayDocumentInANestedArrayCollection";
    private static DocumentDbQueryMappingService queryMapper;
    private static DocumentDbConnectionProperties connectionProperties;
    private static MongoClient client;

    @BeforeAll
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only")
    static void initialize() throws SQLException {
        // Add a valid users to the local MongoDB instance.
        connectionProperties = new DocumentDbConnectionProperties();
        createUser(DATABASE_NAME, USER, PASSWORD);
        connectionProperties.setUser(USER);
        connectionProperties.setPassword(PASSWORD);
        connectionProperties.setDatabase(DATABASE_NAME);
        connectionProperties.setTlsEnabled("false");
        connectionProperties.setHostname("localhost:" + getMongoPort());
        final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ "
                                + "{ \"field\" : 1, \"field1\": \"value\" }, "
                                + "{ \"field\" : 2, \"field2\" : \"value\" , \"field3\" : { \"field4\": 3} } ]}");
        final BsonDocument otherDocument =
                BsonDocument.parse(
                        "{ \"_id\" : \"key1\", \"otherArray\" : [ { \"field\" : 1, \"field3\": \"value\" }, { \"field\" : 2, \"field3\" : \"value\" } ]}");
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        final BsonDocument nestedIddocument =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"document\" : { \"_id\" : 1, \"field1\": \"value\" } }");

        final BsonDocument nestedArrayDocumentInANestedArray =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ "
                                + "{ \"array2\" : [ { \"_id\" : 1, \"field1\": \"value\" } ] } ] }");


        client = createMongoClient(ADMIN_DATABASE, USER, PASSWORD);

        insertBsonDocuments(
                COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{document}, client);
        insertBsonDocuments(
                OTHER_COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{otherDocument}, client);
        insertBsonDocuments(DATE_COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{doc1}, client);
        insertBsonDocuments(NESTED_ID_COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{nestedIddocument}, client);
        insertBsonDocuments(NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{nestedArrayDocumentInANestedArray}, client);
        final DocumentDbDatabaseSchemaMetadata databaseMetadata =
                DocumentDbDatabaseSchemaMetadata.get(connectionProperties, "id", VERSION_NEW, client);
        queryMapper = new DocumentDbQueryMappingService(connectionProperties, databaseMetadata, client);
    }

    @AfterAll
    static void afterAll() throws Exception {
        try (SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(connectionProperties, client)) {
            schemaWriter.remove("id");
        }
        client.close();
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
        Assertions.assertEquals(1, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {\"testCollection__id\": '$_id', \"_id\": 0}}"),
                result.getAggregateOperations().get(0));

        // Get the nested table.
        final String nestedTableQuery =
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, COLLECTION_NAME + "_array");
        result = queryMapper.get(nestedTableQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
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
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\":{\"field\": \"$array.field\", \"_id\": 0}}"),
                result.getAggregateOperations().get(2));

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
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\":{\"renamed\": \"$array.field\", \"_id\": 0}}"),
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
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                                + ": {\"$eq\": [\"$array.field\", {\"$literal\": 1}]}}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
        result.getAggregateOperations().get(5));

        final String queryWithCompoundWhere =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"%s\" = '%s' AND \"%s\" > %s",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field1", "value", "field", 0);
        result = queryMapper.get(queryWithCompoundWhere);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
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
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": 1, "
                                + "\"array_index_lvl_0\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, "
                                + "\"array.field2\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": ["
                                + "{\"$and\": [{\"$eq\": [true, {\"$eq\": [\"$array.field1\", {\"$literal\": \"value\"}]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": "
                                + "[{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, "
                                + "{\"$gt\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}]}, true, "
                                + "{\"$cond\": "
                                + "[{\"$or\": [{\"$eq\": [false, {\"$eq\": [\"$array.field1\", {\"$literal\": \"value\"}]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": "
                                + "[{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, "
                                + "{\"$gt\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}]}, false, null]}]}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(5));
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
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
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
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"1\"}}"), result.getAggregateOperations().get(3));
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
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
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

        final String queryWithDescendingSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" DESC",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field");
        result = queryMapper.get(queryWithDescendingSort);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
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
                BsonDocument.parse("{ \"$sort\": {\"field\": -1 } }"),
                result.getAggregateOperations().get(3));

        final String queryWithCompoundSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC, \"%s\" DESC",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field", "field1");
        result = queryMapper.get(queryWithCompoundSort);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
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
                BsonDocument.parse("{\"$match\": {\"$or\": ["
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
                BsonDocument.parse("{ \"$sort\": {\"field\": 1, \"field1\": -1 } }"),
                result.getAggregateOperations().get(3));
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"_f0\": {\"$sum\": \"$array.field\"}, \"_f1\": {\"$sum\": {\"$cond\": [{\"$ifNull\": [\"$array.field\", false]}, 1, 0]}}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$divide\": [{\"$cond\": [{\"$eq\": [\"$_f1\", {\"$literal\": 0}]}, null, \"$_f0\"]}, \"$_f1\"]}, \"_id\": 0}}"),
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
        Assertions.assertEquals(8, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {\"_id\": \"$_id\", \"array_field\": \"$array.field\", \"array_field1\": \"$array.field1\"}, \"_f3\": {\"$sum\": 1}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": \"$_id._id\", \"array.field\": \"$_id.array_field\", \"array.field1\": \"$_id.array_field1\", \"_f3\": \"$_f3\"}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"_id\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, \""
                                + "_f3\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$and\": ["
                                + "{\"$gt\": [\"$_f3\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 1}, null]}]}, "
                                + "{\"$gt\": [\"$_f3\", {\"$literal\": 1}]}, null]}}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(5));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(6));
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
        Assertions.assertEquals(13, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
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
                                + ": {\"$eq\": [\"$_id\", {\"$literal\": \"key\"}]}}}"),
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
                        "{\"$group\": " +
                                "{\"_id\": {\"_id\": \"$_id\", " +
                                "\"array_field\": \"$array.field\", \"array_field1\": \"$array.field1\"}, " +
                                "\"Total\": {\"$sum\": 1}}}"),
                result.getAggregateOperations().get(5));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {\"_id\": \"$_id._id\", " +
                        "\"array.field\": \"$_id.array_field\", \"array.field1\": \"$_id.array_field1\", \"Total\": \"$Total\"}}"),
                result.getAggregateOperations().get(6));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"_id\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, "
                                + "\"Total\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$and\": ["
                                + "{\"$gt\": [\"$Total\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 1}, null]}]}, "
                                + "{\"$gt\": [\"$Total\", {\"$literal\": 1}]}, null]}}}"),
                result.getAggregateOperations().get(7));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(8));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(9));
        Assertions.assertEquals(
                BsonDocument.parse(
                    "{\"$project\": {\"testCollection__id\": \"$_id\", \"renamed\": \"$array.field\", \"Total\": \"$Total\", \"_id\": 0}}"),
                result.getAggregateOperations().get(10));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$sort\": {\"renamed\": 1}}"),
                result.getAggregateOperations().get(11));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"1\"}}}"),
                result.getAggregateOperations().get(12));
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
        Assertions.assertEquals(4, innerJoinResult.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {\"testCollection__id0\": {\"$cond\": [{\"$or\": [{\"$ifNull\": [\"$array.field\", false]}, " +
                                "{\"$ifNull\": [\"$array.field1\", false]}, {\"$ifNull\": [\"$array.field2\", false]}]}, \"$_id\", null]}, \"_id\": \"$_id\"}}"),
                innerJoinResult.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                innerJoinResult.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                innerJoinResult.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"testCollection__id\": \"$_id\", \"testCollection__id0\": \"$testCollection__id0\", \"array_index_lvl_0\": \"$array_index_lvl_0\", \"field\": \"$array.field\", \"field1\": \"$array.field1\", \"field2\": \"$array.field2\", \"_id\": 0}}"),
                innerJoinResult.getAggregateOperations().get(3));
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
        Assertions.assertEquals(3, leftJoinResult.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {\"testCollection__id0\": {\"$cond\": [{\"$or\": [{\"$ifNull\": [\"$array.field\", false]}, " +
                                "{\"$ifNull\": [\"$array.field1\", false]}, {\"$ifNull\": [\"$array.field2\", false]}]}, \"$_id\", null]}, \"_id\": \"$_id\"}}"),
                leftJoinResult.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                leftJoinResult.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"testCollection__id0\": \"$testCollection__id0\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                leftJoinResult.getAggregateOperations().get(2));
    }

    @Test
    @DisplayName("Tests that a statement with join works for three tables from the same collection.")
    void testSameCollectionJoinWithTwoLevelNestedArray() {
        final String twoInnerJoins =
                String.format(
                        "SELECT field1 FROM \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\" "
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\""
                                + "AND \"%s\".\"%s\" = \"%s\".\"%s\"",
                        DATABASE_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME,
                        DATABASE_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "__id",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "__id",
                        DATABASE_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array_array2",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array_array2",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "__id",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "__id",

                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array_array2",
                        "array_index_lvl_0",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array",
                        "array_index_lvl_0");

        Assertions.assertDoesNotThrow(() -> queryMapper.get(twoInnerJoins));
    }

    @Test
    @DisplayName("Tests that a statement with join works for two tables from the same collection using only _id.")
    void testSameCollectionJoinWithTwoLevelNestedArrayUsingOnlyPrimaryKeys() {

        final String innerJoinRootDocumentWithNestedNestedArray =
                String.format(
                        "SELECT field1 FROM \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                        DATABASE_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME,
                        DATABASE_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array_array2",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "__id",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array_array2",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "__id");

        Assertions.assertDoesNotThrow(() -> queryMapper.get(innerJoinRootDocumentWithNestedNestedArray));
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
        Assertions.assertEquals(14, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {\"testCollection__id0\": {\"$cond\": [{\"$or\": [{\"$ifNull\": [\"$array.field2\", false]}, " +
                                "{\"$ifNull\": [\"$array.field\", false]}, {\"$ifNull\": [\"$array.field1\", false]}]}, \"$_id\", null]}, \"_id\": \"$_id\"}}"),
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
                        "{\"$match\": {\"$or\": [{\"array.field2\": {\"$exists\": true}}, {\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"_id\": 1, "
                                + "\"testCollection__id0\": 1, "
                                + "\"array.field\": 1, \"array.field1\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 1}, null]}]}, {\"$gt\": [\"$array.field\", "
                                + "{\"$literal\": 1}]}, null]}}}"),
        result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}}"),
                result.getAggregateOperations().get(5));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {\"_id\": \"$_id\", \"array_field\": \"$array.field\", \"array_field1\": \"$array.field1\"}, \"Total\": {\"$sum\": 1}}}"),
                result.getAggregateOperations().get(6));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": \"$_id._id\", \"array.field\": \"$_id.array_field\", \"array.field1\": \"$_id.array_field1\", \"Total\": \"$Total\"}}"),
                result.getAggregateOperations().get(7));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": 1, \"array.field\": 1, \"array.field1\": 1, \"Total\": 1, \"placeholderField1F84EB1G3K47\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$Total\", null]}, {\"$gt\": [{\"$literal\": 1}, null]}]}, {\"$gt\": [\"$Total\", {\"$literal\": 1}]}, null]}}}"),
                result.getAggregateOperations().get(8));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}}"),
                result.getAggregateOperations().get(9));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(10));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {\"renamed\": \"$array.field\", \"Total\": \"$Total\", \"_id\": 0}}"),
                result.getAggregateOperations().get(11));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$sort\": {\"renamed\": 1}}"),
                result.getAggregateOperations().get(12));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"1\"}}"),
                result.getAggregateOperations().get(13));
    }

    @Test
    @DisplayName("Tests that a valid query that cannot be executed purely with aggregate throws an exception.")
    void testUnsupportedQuery() {
        // Union requires 2 separate calls.
        final String query =
                String.format("SELECT * FROM \"%s\".\"%s\" UNION SELECT \"%s\" FROM \"%s\".\"%s\"",
                        DATABASE_NAME, COLLECTION_NAME, COLLECTION_NAME + "__id", DATABASE_NAME, COLLECTION_NAME + "_array");
        Assertions.assertEquals(SqlError.lookup(SqlError.UNSUPPORTED_SQL, query),
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
        Assertions.assertEquals(String.format("Unable to parse SQL"
                        + " 'SELECT * FROM \"database\".\"testCollection\" UNION SELECT * FROM \"database\".\"testCollection_array\"'.%n"
                        + " Reason: 'At line 1, column 56: Column count mismatch in UNION'"),
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
                        "{\"$match\": {\"$or\": "
                                + "[{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}] }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$lookup\": {"
                                + "\"from\": \"otherTestCollection\", "
                                + "\"let\": {\"field1\": \"$array.field1\", \"field\": \"$array.field\", \"array_index_lvl_0\": \"$array_index_lvl_0\", \"field2\": \"$array.field2\", \"testCollection__id\": \"$_id\"}, "
                                + "\"pipeline\": ["
                                + "{\"$unwind\": {\"path\": \"$otherArray\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"otherArray_index_lvl_0\"}}, "
                                + "{\"$match\": {\"$or\": [{\"otherArray.field\": {\"$exists\": true}}, {\"otherArray.field3\": {\"$exists\": true}}]}}, "
                                + "{\"$match\": {\"$expr\": {\"$eq\": [\"$$testCollection__id\", \"$_id\"]}}}], "
                                + "\"as\": \"otherTestCollection_otherArray\"}}"),
        result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$otherTestCollection_otherArray\", \"preserveNullAndEmptyArrays\": false}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"otherTestCollection__id\": \"$otherTestCollection_otherArray._id\", "
                                + "\"otherArray_index_lvl_0\": \"$otherTestCollection_otherArray.otherArray_index_lvl_0\", "
                                + "\"field0\": \"$otherTestCollection_otherArray.otherArray.field\", "
                                + "\"field3\": \"$otherTestCollection_otherArray.otherArray.field3\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(4));

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
                        "{\"$match\": {\"$or\": "
                                + "[{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}] }}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$lookup\": {"
                                + "\"from\": \"otherTestCollection\", "
                                + "\"let\": {\"field1\": \"$array.field1\", \"field\": \"$array.field\", \"array_index_lvl_0\": \"$array_index_lvl_0\", \"field2\": \"$array.field2\", \"testCollection__id\": \"$_id\"}, "
                                + "\"pipeline\": ["
                                + "{\"$unwind\": {\"path\": \"$otherArray\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"otherArray_index_lvl_0\"}}, "
                                + "{\"$match\": {\"$or\": [{\"otherArray.field\": {\"$exists\": true}}, {\"otherArray.field3\": {\"$exists\": true}}]}}, "
                                + "{\"$match\": {\"$expr\": {\"$eq\": [\"$$testCollection__id\", \"$_id\"]}}}], "
                                + "\"as\": \"otherTestCollection_otherArray\"}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$otherTestCollection_otherArray\", \"preserveNullAndEmptyArrays\": true}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"otherTestCollection__id\": \"$otherTestCollection_otherArray._id\", "
                                + "\"otherArray_index_lvl_0\": \"$otherTestCollection_otherArray.otherArray_index_lvl_0\", "
                                + "\"field0\": \"$otherTestCollection_otherArray.otherArray.field\", "
                                + "\"field3\": \"$otherTestCollection_otherArray.otherArray.field3\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(4));
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
        Assertions.assertEquals(13, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                            "{\"$lookup\": {"
                                    + "\"from\": \"testCollection\", "
                                    + "\"let\": {\"otherTestCollection__id\": \"$_id\"}, "
                                    + "\"pipeline\": ["
                                    + "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}, "
                                    + "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}, "
                                    + "{\"$match\": {\"$expr\": {\"$eq\": [\"$$otherTestCollection__id\", \"$_id\"]}}}], \"as\": \"testCollection_array\"}}"),
        result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$unwind\": {\"path\": \"$testCollection_array\", \"preserveNullAndEmptyArrays\": false}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                "{\"$project\": {\"_id\": 1, \"testCollection_array._id\": 1, \"testCollection_array.array.field\": 1, \"testCollection_array.array.field1\": 1, \"placeholderField1F84EB1G3K47\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$testCollection_array.array.field\", null]}, {\"$gt\": [{\"$literal\": 1}, null]}]}, {\"$gt\": [\"$testCollection_array.array.field\", {\"$literal\": 1}]}, null]}}}"),
        result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(
            BsonDocument.parse(
                "{\"$group\": {\"_id\": {\"_id\": \"$_id\", \"testCollection_array_array_field\": \"$testCollection_array.array.field\", \"testCollection_array_array_field1\": \"$testCollection_array.array.field1\"}, \"Total\": {\"$sum\": 1}}}"),
            result.getAggregateOperations().get(5));
        Assertions.assertEquals(
            BsonDocument.parse(
                "{\"$project\": {\"_id\": \"$_id._id\", \"testCollection_array.array.field\": \"$_id.testCollection_array_array_field\", \"testCollection_array.array.field1\": \"$_id.testCollection_array_array_field1\", \"Total\": \"$Total\"}}"),
            result.getAggregateOperations().get(6));
        Assertions.assertEquals(
            BsonDocument.parse(
                "{\"$project\": {\"_id\": 1, \"testCollection_array.array.field\": 1, \"testCollection_array.array.field1\": 1, \"Total\": 1, \"placeholderField1F84EB1G3K47\": {\"$cond\": [{\"$and\": [{\"$gt\": [\"$Total\", null]}, {\"$gt\": [{\"$literal\": 1}, null]}]}, {\"$gt\": [\"$Total\", {\"$literal\": 1}]}, null]}}}"),
        result.getAggregateOperations().get(7));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(8));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(9));
        Assertions.assertEquals(
                BsonDocument.parse(
                    "{\"$project\": {\"renamed\": \"$testCollection_array.array.field\", \"Total\": \"$Total\", \"_id\": 0}}"),
                    result.getAggregateOperations().get(10));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$sort\": {\"renamed\": 1}}"),
                result.getAggregateOperations().get(11));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": {\"$numberLong\": \"1\"}}"), result.getAggregateOperations().get(12));


    }

    @Test
    @DisplayName("Tests that unsupported join conditions or types throw an exception.")
    void testUnsupportedJoins() {
        // Cannot do right join on tables from different collections.
        final String rightJoinQuery =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\""
                                + " RIGHT JOIN \"%s\".\"%s\""
                                + " ON \"%s\" = \"%s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        DATABASE_NAME,
                        OTHER_COLLECTION_NAME + "_otherArray",
                        COLLECTION_NAME + "__id",
                        OTHER_COLLECTION_NAME + "__id");

        String message = Assertions
                        .assertThrows(SQLException.class, () -> queryMapper.get(rightJoinQuery))
                        .getMessage();
        Assertions.assertTrue(message.contains("Unable to parse SQL"));
        Assertions.assertTrue(message.contains(SqlError.lookup(SqlError.UNSUPPORTED_JOIN_TYPE, "RIGHT")));


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
        message = Assertions.assertThrows(SQLException.class, () -> queryMapper.get(fullJoinQuery))
                .getMessage();
        Assertions.assertTrue(message.contains("Unable to parse SQL"));
        Assertions.assertTrue(message.contains(SqlError.lookup(SqlError.UNSUPPORTED_JOIN_TYPE, "FULL")));

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
        message = Assertions.assertThrows(SQLException.class, () -> queryMapper.get(multipleConditionsQuery))
                .getMessage();
        Assertions.assertTrue(message.contains("Unable to parse SQL"));
        Assertions.assertTrue(message.contains(SqlError.lookup(SqlError.SINGLE_EQUIJOIN_ONLY)));

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
        message = Assertions.assertThrows(SQLException.class, () -> queryMapper.get(nonForeignKeyQuery))
                .getMessage();
        Assertions.assertTrue(message.contains("Unable to parse SQL"));
        Assertions.assertTrue(message.contains(SqlError.lookup(SqlError.EQUIJOINS_ON_FK_ONLY)));

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
        message = Assertions.assertThrows(SQLException.class, () -> queryMapper.get(nonEqualityQuery))
                .getMessage();
        Assertions.assertTrue(message.contains("Unable to parse SQL"));
        Assertions.assertTrue(message.contains(SqlError.lookup(SqlError.EQUIJOINS_ON_FK_ONLY)));

        final String innerJoin =
                String.format(
                        "SELECT field1 FROM \"%s\".\"%s\""
                                + "INNER JOIN \"%s\".\"%s\""
                                + "ON \"%s\".\"%s\" = \"%s\".\"%s\"",
                        DATABASE_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array",
                        DATABASE_NAME,
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array_array2",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "__id",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "_array_array2",
                        NESTED_DOCUMENT_IN_NESTED_ARRAY_COLLECTION_NAME + "__id");

        message = Assertions.assertThrows(SQLException.class, () -> queryMapper.get(innerJoin))
                .getMessage();
        Assertions.assertTrue(message.contains(SqlError.lookup(SqlError.JOIN_MISSING_PRIMARY_KEYS,"[array_index_lvl_0]")));
    }

    @Test
    @DisplayName("Tests SUM(1), and that field names generated by Calcite have $ symbols removed.")
    void testQueryWithSumOne() throws SQLException {
        final String query =
                String.format(
                        "SELECT SUM(1) FROM \"%s\".\"%s\"", DATABASE_NAME, COLLECTION_NAME);
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse("{\"$project\": {\"_f0\": {\"$literal\": 1}, \"_id\": 0}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$group\": {\"_id\": {}, \"EXPR$0\": {\"$sum\": \"$_f0\"}}}"),
                result.getAggregateOperations().get(1));
    }

    @Test
    @DisplayName("Tests CASE with one field, and three sections.")
    void testQueryWithCASE() throws SQLException {
        final String query =
                String.format(
                        "SELECT CASE " +
                                "WHEN \"field\" > 10 THEN 'A' " +
                                "WHEN \"field\" > 5 THEN 'B' " +
                                "ELSE 'C' END FROM \"%s\".\"%s\"", DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"EXPR$0\": {\"$cond\": [{\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, " +
                                "{\"$gt\": [{\"$literal\": 10}, null]}]}, " +
                                "{\"$gt\": [\"$array.field\", {\"$literal\": 10}]}, null]}, {\"$literal\": \"A\"}, " +
                                "{\"$cond\": [{\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [{\"$literal\": 5}, null]}]}, " +
                                "{\"$gt\": [\"$array.field\", {\"$literal\": 5}]}, null]}, {\"$literal\": \"B\"}, {\"$literal\": \"C\"}]}]}, "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
    }

    @Test
    @DisplayName("Tests a query with a where clause comparing two literals.")
    void testWhereTwoLiterals() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE 2 > 1", DATABASE_NAME, COLLECTION_NAME);
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(1, result.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"testCollection__id\": \"$_id\", \"_id\": 0}}"),
                result.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Tests queries with SUBSTRING")
    void testQuerySubstring() throws SQLException {
        final String query =
                String.format(
                        "SELECT SUBSTRING(\"field\", 4, 2) FROM \"%s\".\"%s\" WHERE SUBSTRING(\"field\", 2, 3) = 'abc'" , DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{ \"$unwind\": {"
                                + "\"path\": \"$array\", "
                                + "\"includeArrayIndex\" : \"array_index_lvl_0\", "
                                + "\"preserveNullAndEmptyArrays\": true }}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$match\": {\"$or\": ["
                        + "{\"array.field\": {\"$exists\": true}}, "
                        + "{\"array.field1\": {\"$exists\": true}}, "
                        + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {\"_id\": 1, \"array_index_lvl_0\": 1, \"array.field\": 1, \"array.field1\": 1, \"array.field2\": 1, \"placeholderField1F84EB1G3K47\": {\"$eq\": [{\"$substrCP\": [\"$array.field\", {\"$subtract\": [{\"$literal\": 2}, 1]}, {\"$literal\": 3}]}, {\"$literal\": \"abc\"}]}}}"),
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
                        "{\"$project\": {\"EXPR$0\": {\"$substrCP\": [\"$array.field\", {\"$subtract\": [{\"$literal\": 4}, 1]}, {\"$literal\": 2}]}, \"_id\": 0}}"),
                result.getAggregateOperations().get(5));
    }

    @Test
    @DisplayName("Tests queries with substring containing expressions works.")
    void testQuerySubstringExpr() throws SQLException {
        final String query =
                String.format(
                        "SELECT SUBSTRING(\"field\", \"field2\", \"field1\" - \"field2\") " +
                                "FROM \"%s\".\"%s\" WHERE SUBSTRING(\"field\", \"field2\", \"field1\" + \"field2\") = 'abcd'",
                        DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
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
                                + ": {\"$eq\": [{\"$substrCP\": [\"$array.field\", "
                                + "{\"$subtract\": [\"$array.field2\", 1]},"
                                + " {\"$add\": [\"$array.field1\", \"$array.field2\"]}]}, "
                                + "{\"$literal\": \"abcd\"}]}}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": {\"$eq\": true}}}"),
                result.getAggregateOperations().get(3));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {" + DocumentDbFilter.BOOLEAN_FLAG_FIELD + ": 0}}"),
                result.getAggregateOperations().get(4));
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {"
                        + "\"EXPR$0\": {\"$substrCP\": [\"$array.field\", {\"$subtract\": [\"$array.field2\", 1]}, {\"$subtract\": [\"$array.field1\", \"$array.field2\"]}]}, \"_id\": 0}}"),
                result.getAggregateOperations().get(5));
    }

    @Test
    @DisplayName("Tests that unquoted identifiers retain their casing but are evaluated case-sensitively.")
    void testQueryWithUnquotedIdentifiers() throws SQLException {
        final String correctCasing =
                String.format("SELECT * FROM %s.%s", DATABASE_NAME, COLLECTION_NAME);
        final DocumentDbMqlQueryContext result = queryMapper.get(correctCasing);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(1, result.getAggregateOperations().size());

        final String incorrectCasing =
                String.format("SELECT * FROM %s.%s", DATABASE_NAME, COLLECTION_NAME.toUpperCase());
        Assertions.assertEquals(String.format(
                "Unable to parse SQL 'SELECT * FROM database.TESTCOLLECTION'.%n"
                        + " Reason: 'From line 1, column 15 to line 1, column 37:"
                        + " Object 'TESTCOLLECTION' not found within 'database'; did you mean 'testCollection'?'"),
                Assertions.assertThrows(SQLException.class, () -> queryMapper.get(incorrectCasing))
                        .getMessage());
    }

    @Test
    @DisplayName("Tests queries with where clause containing nested AND.")
    void testQueryAndWithTypes() throws SQLException {
        final String query =
                String.format(
                        "SELECT \"field\" > '2021-01-01' AND \"field\" < '2020-02-01' FROM \"%s\".\"%s\"",
                        DATABASE_NAME, DATE_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(DATE_COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(1, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"EXPR$0\": {\"$cond\": [{\"$and\": [{\"$eq\": [true, {\"$cond\": ["
                                + "{\"$and\": [{\"$gt\": [\"$field\", null]}, "
                                + "{\"$gt\": [{\"$date\": \"2021-01-01T00:00:00Z\"}, null]}]}, "
                                + "{\"$gt\": [\"$field\", {\"$date\": \"2021-01-01T00:00:00Z\"}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}, "
                                + "{\"$gt\": [{\"$date\": \"2020-02-01T00:00:00Z\"}, null]}]}, "
                                + "{\"$lt\": [\"$field\", {\"$date\": \"2020-02-01T00:00:00Z\"}]}, null]}]}]}, true, "
                                + "{\"$cond\": [{\"$or\": [{\"$eq\": [false, "
                                + "{\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}, "
                                + "{\"$gt\": [{\"$date\": \"2021-01-01T00:00:00Z\"}, null]}]}, "
                                + "{\"$gt\": [\"$field\", {\"$date\": \"2021-01-01T00:00:00Z\"}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$field\", null]}, "
                                + "{\"$gt\": [{\"$date\": \"2020-02-01T00:00:00Z\"}, null]}]}, "
                                + "{\"$lt\": [\"$field\", {\"$date\": \"2020-02-01T00:00:00Z\"}]}, null]}]}]}, false, null]}]}"
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(0));
    }

    @Test
    @DisplayName("Tests querying when select list exceeds max field limit for $project.")
    void testLargeSelectList() throws SQLException {
        final String query = String.format(
                "SELECT \"%1$s\" AS \"1\", \"%1$s\" AS \"2\", \"%1$s\" AS \"3\", \"%1$s\" AS \"4\",  \"%1$s\" AS \"5\", "
                        + "\"%1$s\" AS \"6\", \"%1$s\" AS \"7\", \"%1$s\" AS \"8\", \"%1$s\" AS \"9\",  \"%1$s\" AS \"10\","
                        + "\"%1$s\" AS \"11\", \"%1$s\" AS \"12\", \"%1$s\" AS \"13\", \"%1$s\" AS \"14\",  \"%1$s\" AS \"15\", "
                        + "\"%1$s\" AS \"16\", \"%1$s\" AS \"17\", \"%1$s\" AS \"18\", \"%1$s\" AS \"19\",  \"%1$s\" AS \"20\", "
                        + "\"%1$s\" AS \"21\", \"%1$s\" AS \"22\", \"%1$s\" AS \"23\", \"%1$s\" AS \"24\",  \"%1$s\" AS \"25\", "
                        + "\"%1$s\" AS \"26\", \"%1$s\" AS \"27\", \"%1$s\" AS \"28\", \"%1$s\" AS \"29\",  \"%1$s\" AS \"30\","
                        + "\"%1$s\" AS \"31\", \"%1$s\" AS \"32\", \"%1$s\" AS \"33\", \"%1$s\" AS \"34\",  \"%1$s\" AS \"35\", "
                        + "\"%1$s\" AS \"36\", \"%1$s\" AS \"37\", \"%1$s\" AS \"38\", \"%1$s\" AS \"39\",  \"%1$s\" AS \"40\", "
                        + "\"%1$s\" AS \"41\", \"%1$s\" AS \"42\", \"%1$s\" AS \"43\", \"%1$s\" AS \"44\",  \"%1$s\" AS \"45\", "
                        + "\"%1$s\" AS \"46\", \"%1$s\" AS \"47\", \"%1$s\" AS \"48\", \"%1$s\" AS \"49\",  \"%1$s\" AS \"50\","
                        + "\"%1$s\" AS \"51\" FROM \"%2$s\".\"%3$s\"",
            "field", DATABASE_NAME, DATE_COLLECTION_NAME);
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertEquals(DATE_COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(51, result.getColumnMetaData().size());
        Assertions.assertEquals(0, result.getAggregateOperations().size());
    }

    @Test
    @DisplayName("Tests that $addFields operation is added before $unwind except when $unwind is needed for the $addFields.")
    void testJoinOpOrder() throws SQLException {
        String query =
                String.format(
                        "SELECT * FROM \"%1$s\".\"%2$s\""
                                + " LEFT JOIN \"%1$s\".\"%3$s\" ON \"%2$s\".\"%2$s__id\" = \"%3$s\".\"%2$s__id\"",
                        DATABASE_NAME,
                        COLLECTION_NAME,
                        COLLECTION_NAME + "_array");
        DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(6, result.getColumnMetaData().size());
        Assertions.assertEquals(3, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {\"testCollection__id0\": {\"$cond\": [{\"$or\": ["
                                + "{\"$ifNull\": [\"$array.field\", false]}, "
                                + "{\"$ifNull\": [\"$array.field1\", false]}, {\"$ifNull\": [\"$array.field2\", false]}]}, \"$_id\", null]}, "
                                + "\"_id\": \"$_id\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                         "{\"$project\": {\"testCollection__id\": \"$_id\", "
                                 + "\"testCollection__id0\": \"$testCollection__id0\", "
                                 + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                 + "\"field\": \"$array.field\", "
                                 + "\"field1\": \"$array.field1\", "
                                 + "\"field2\": \"$array.field2\", "
                                 + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        query =
                String.format(
                        "SELECT * FROM \"%1$s\".\"%2$s\" "
                                + "LEFT OUTER JOIN \"%1$s\".\"%3$s\" "
                                + "ON \"%2$s\".\"%4$s\" = \"%3$s\".\"%4$s\" "
                                + "AND \"%2$s\".\"%5$s\" = \"%3$s\".\"%5$s\"",
                        DATABASE_NAME,
                        COLLECTION_NAME + "_array",
                        COLLECTION_NAME + "_array_field3",
                        COLLECTION_NAME + "__id",
                        "array_index_lvl_0");
        result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(8, result.getColumnMetaData().size());
        Assertions.assertEquals(4, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$addFields\": {"
                                + "\"testCollection__id0\": {\"$cond\": [{\"$ifNull\": [\"$array.field3.field4\", false]}, \"$_id\", null]}, "
                                + "\"_id\": {\"$cond\": [{\"$or\": [{\"$ifNull\": [\"$array.field\", false]}, {\"$ifNull\": [\"$array.field1\", false]}, {\"$ifNull\": [\"$array.field2\", false]}]}, \"$_id\", null]}, "
                                + "\"array_index_lvl_00\": {\"$cond\": [{\"$ifNull\": [\"$array.field3.field4\", false]}, \"$array_index_lvl_0\", null]}, "
                                + "\"array_index_lvl_0\": {\"$cond\": [{\"$or\": [{\"$ifNull\": [\"$array.field\", false]}, {\"$ifNull\": [\"$array.field1\", false]}, {\"$ifNull\": [\"$array.field2\", false]}]}, \"$array_index_lvl_0\", null]}}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": ["
                                + "{\"array.field\": {\"$exists\": true}}, "
                                + "{\"array.field1\": {\"$exists\": true}}, "
                                + "{\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"testCollection__id0\": \"$testCollection__id0\", "
                                + "\"array_index_lvl_00\": \"$array_index_lvl_00\", "
                                + "\"field4\": \"$array.field3.field4\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(3));
    }

    @Test
    @DisplayName("Tests that fields can be selected with the column name '_id' ")
    void testIdAsColumnName() throws SQLException {
        // Get a base table with a rename.
        final String basicQuery =
                String.format("SELECT \"testCollection__id\" AS \"_id\" FROM \"%s\".\"%s\"", DATABASE_NAME, COLLECTION_NAME);
        DocumentDbMqlQueryContext result = queryMapper.get(basicQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(1, result.getAggregateOperations().size());

        // Make sure there is no $_id: 0 here.
        Assertions.assertEquals(BsonDocument.parse("{\"$project\": {\"_id\": '$_id'} }"), result.getAggregateOperations().get(0));

        // Get a table with nested _id.
        final String nestedTableQuery =
                String.format("SELECT * FROM \"%s\".\"%s\"",
                        DATABASE_NAME,
                        NESTED_ID_COLLECTION_NAME + "_document");
        result = queryMapper.get(nestedTableQuery);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(NESTED_ID_COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(3, result.getColumnMetaData().size());
        Assertions.assertEquals(2, result.getAggregateOperations().size());

        // Make sure there is no $_id: 0 here.
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"nestedIdCollection__id\": \"$_id\", \"_id\": \"$document._id\", \"field1\": \"$document.field1\"}}"),
                result.getAggregateOperations().get(1));
    }
}
