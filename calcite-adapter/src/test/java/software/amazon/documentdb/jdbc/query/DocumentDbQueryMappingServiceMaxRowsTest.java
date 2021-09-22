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
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.sql.SQLException;
import java.time.Instant;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_NEW;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryMappingServiceMaxRowsTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String COLLECTION_NAME = "testCollection";
    private static final String OTHER_COLLECTION_NAME = "otherTestCollection";
    private static final String DATE_COLLECTION_NAME = "dateTestCollection";
    private static final String NESTED_ID_COLLECTION_NAME = "nestedIdCollection";
    private static final String COLLECTION_EXTRA_FIELD = "fieldTestCollection";
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
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");

        final BsonDocument otherDocument =
                BsonDocument.parse(
                        "{ \"_id\" : \"key1\", \"otherArray\" : [ { \"field\" : 1, \"field3\": \"value\" }, { \"field\" : 2, \"field3\" : \"value\" } ]}");
        final BsonDocument doc1 = BsonDocument.parse("{\"_id\": 101}");
        doc1.append("field", new BsonDateTime(dateTime));
        final BsonDocument document3 =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"fieldA\": 3, \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        final BsonDocument nestedIddocument =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"document\" : { \"_id\" : 1, \"field1\": \"value\" } }");

        client = createMongoClient(ADMIN_DATABASE, USER, PASSWORD);

        insertBsonDocuments(
                COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{document}, client);
        insertBsonDocuments(
                OTHER_COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{otherDocument}, client);
        insertBsonDocuments(DATE_COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{doc1}, client);
        insertBsonDocuments(
                COLLECTION_EXTRA_FIELD, DATABASE_NAME, new BsonDocument[]{document3}, client);
        insertBsonDocuments(NESTED_ID_COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{nestedIddocument}, client);
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
    @DisplayName("Tests $limit is produced when max rows is passed.")
    void testMaxRows() throws SQLException {
        final String query =
                String.format("SELECT * FROM \"%s\".\"%s\"", DATABASE_NAME, COLLECTION_NAME);
        final DocumentDbMqlQueryContext result = queryMapper.get(query, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getColumnMetaData().size());
        Assertions.assertEquals(2, result.getAggregateOperations().size());
        Assertions.assertEquals(BsonDocument.parse(
                "{\"$project\": {\"testCollection__id\": '$_id', \"_id\": 0}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 10}"), result.getAggregateOperations().get(1));
    }

    @Test
    @DisplayName("Tests $limit and $sort are produced when max rows is set.")
    void testOrderByWithMaxRows() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
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
                BsonDocument.parse("{\"$limit\": 10}"), result.getAggregateOperations().get(4));
    }

    @Test
    @DisplayName("Tests $limit and $sort are produced when max rows is set and the query has a limit.")
    void testOrderByWithMaxRowsAndLimit() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC LIMIT 5",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
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
                BsonDocument.parse("{\"$limit\": 5}"), result.getAggregateOperations().get(4));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 10}"), result.getAggregateOperations().get(5));
    }

    @Test
    @DisplayName("Tests $limit and $sort are produced when max rows is set and the subquery has a limit.")
    void testOrderByWithMaxRowsAndInnerLimit() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM ( SELECT * FROM \"%s\".\"%s\" LIMIT 20 ) ORDER BY \"%s\" ASC",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(7, result.getAggregateOperations().size());
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
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 20}"), result.getAggregateOperations().get(3));
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
                BsonDocument.parse("{\"$limit\": 10}"), result.getAggregateOperations().get(6));
    }

    @Test
    @DisplayName("Tests $limit and $sort are produced when max rows is set and the sub query and the query has limit.")
    void testOrderByWithMaxRowsAndInnerLimitAndOuterLimit() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM  (SELECT * FROM \"%s\".\"%s\" LIMIT 20)  ORDER BY \"%s\" ASC LIMIT 30",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(8, result.getAggregateOperations().size());
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
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 20}"), result.getAggregateOperations().get(3));
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
                BsonDocument.parse("{\"$limit\": 30}"), result.getAggregateOperations().get(6));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 10}"), result.getAggregateOperations().get(7));
    }

    @Test
    @DisplayName("Tests $limit is produced and $sort is omitted when max rows is set and the sub query and the query has  no limit.")
    void testOrderByWithMaxRowsAndInnerOrderBy() throws SQLException {
        final String queryWithAscendingSort =
                String.format(
                        "SELECT * FROM  (SELECT * FROM \"%s\".\"%s\" ORDER BY \"%s\" ASC) ",
                        DATABASE_NAME, COLLECTION_NAME + "_array", "field");
        final DocumentDbMqlQueryContext result = queryMapper.get(queryWithAscendingSort, 10);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.getColumnMetaData().size());
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
                        "{\"$project\": "
                                + "{\"testCollection__id\": \"$_id\", "
                                + "\"array_index_lvl_0\": \"$array_index_lvl_0\", "
                                + "\"field\": \"$array.field\", "
                                + "\"field1\": \"$array.field1\", "
                                + "\"field2\": \"$array.field2\", "
                                + "\"_id\": 0}}"),
                result.getAggregateOperations().get(2));
        Assertions.assertEquals(
                BsonDocument.parse("{\"$limit\": 10}"), result.getAggregateOperations().get(3));
    }
}
