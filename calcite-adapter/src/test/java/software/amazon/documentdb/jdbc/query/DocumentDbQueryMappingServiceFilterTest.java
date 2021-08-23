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
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;
import java.sql.SQLException;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_NEW;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryMappingServiceFilterTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String COLLECTION_NAME = "testCollection";
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
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ { \"field\" : 1, \"field1\": \"value\" }, { \"field\" : 2, \"field2\" : \"value\" } ]}");
        client = createMongoClient(ADMIN_DATABASE, USER, PASSWORD);

        insertBsonDocuments(
                COLLECTION_NAME, DATABASE_NAME, new BsonDocument[]{document}, client);
        final DocumentDbDatabaseSchemaMetadata databaseMetadata =
                DocumentDbDatabaseSchemaMetadata.get(connectionProperties, "id", VERSION_NEW, client);
        queryMapper = new DocumentDbQueryMappingService(connectionProperties, databaseMetadata, client);
    }

    @AfterAll
    static void afterAll() throws Exception {
        try (SchemaWriter schemaWriter = SchemaStoreFactory
                .createWriter(connectionProperties, client)) {
            schemaWriter.remove("id");
        }
        client.close();
    }

    @Test
    @DisplayName("Test queries with WHERE f1 IN (c1, c2...)")
    void testQueryWithIn() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" IN (2, 3)" , DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\":"
                                + " {\"_id\": 1, "
                                + "\"array_index_lvl_0\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, "
                                + "\"array.field2\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$or\": [{\"$eq\": [true, {\"$eq\": [\"$array.field\", {\"$literal\": 2}]}]}, "
                                + "{\"$eq\": [true, {\"$eq\": [\"$array.field\", {\"$literal\": 3}]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$eq\": [\"$array.field\", {\"$literal\": 2}]}]}, "
                                + "{\"$eq\": [false, {\"$eq\": [\"$array.field\", {\"$literal\": 3}]}]}]}, false, null]}]}}}"),
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
    @DisplayName("Test queries with WHERE f1 NOT IN (c1, c2...)")
    void testQueryWithNotIn() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" NOT IN (2, 3)" , DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
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
                                + ": {\"$cond\": [{\"$and\": [{\"$eq\": [true, "
                                + "{\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 2}, null]}]}, {\"$ne\": [\"$array.field\", {\"$literal\": 2}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [{\"$literal\": 3}, null]}]}, "
                                + "{\"$ne\": [\"$array.field\", {\"$literal\": 3}]}, null]}]}]}, true, {\"$cond\": [{\"$or\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [{\"$literal\": 2}, null]}]}, {\"$ne\": [\"$array.field\", {\"$literal\": 2}]}, null]}]}, {\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [{\"$literal\": 3}, null]}]}, {\"$ne\": [\"$array.field\", {\"$literal\": 3}]}, null]}]}]}, false, null]}]}}}"),
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
    @DisplayName("Tests queries with IS [NOT] NULL")
    void testQueryIsNull() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" WHERE \"field\" IS NULL OR \"field1\" IS NOT NULL" , DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
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
                                + ": {\"$cond\": [{\"$or\": [{\"$eq\": [true, {\"$lte\": [\"$array.field\", null]}]}, "
                                + "{\"$eq\": [true, {\"$gt\": [\"$array.field1\", null]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$lte\": [\"$array.field\", null]}]}, "
                                + "{\"$eq\": [false, {\"$gt\": [\"$array.field1\", null]}]}]}, false, null]}]}}}"),
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
    @DisplayName("Tests queries with where clause containing arithmetic.")
    void testQueryArithmeticWhere() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" * \"field1\" / \"field2\" + \"field1\" - \"field2\" = 7",
                        DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
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
                                + ": {\"$eq\": [{\"$subtract\": ["
                                + "{\"$add\": [{\"$divide\": [{\"$multiply\": [\"$array.field\", \"$array.field1\"]}, "
                                + "\"$array.field2\"]}, \"$array.field1\"]}, \"$array.field2\"]}, {\"$literal\": 7}]}}}"),
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
                        DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
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
                                + ": {\"$cond\": [{\"$or\": [{\"$eq\": [true, "
                                + "{\"$eq\": [{\"$mod\": [\"$array.field\", {\"$literal\": 3}]}, {\"$literal\": 2}]}]}, "
                                + "{\"$eq\": [true, {\"$eq\": [{\"$mod\": [{\"$literal\": 8}, \"$array.field\"]}, {\"$literal\": 2}]}]}, "
                                + "{\"$eq\": [true, {\"$eq\": [{\"$literal\": 1}, \"$array.field\"]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$eq\": [{\"$mod\": [\"$array.field\", {\"$literal\": 3}]}, {\"$literal\": 2}]}]}, "
                                + "{\"$eq\": [false, {\"$eq\": [{\"$mod\": [{\"$literal\": 8}, \"$array.field\"]}, {\"$literal\": 2}]}]}, "
                                + "{\"$eq\": [false, {\"$eq\": [{\"$literal\": 1}, \"$array.field\"]}]}]}, false, null]}]}}}"),
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
                                "WHERE \"field\" > 0 OR (\"field1\" > 0OR \"field2\" > 6)",
                        DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
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
                                + ": {\"$cond\": [{\"$or\": ["
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, "
                                + "{\"$gt\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$gt\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]},"
                                + " {\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$gt\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, {\"$gt\": [{\"$literal\": 0}, null]}]}, "
                                + "{\"$gt\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, {\"$eq\": [false, "
                                + "{\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, "
                                + "{\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, false, null]}]}}}"),
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
    @DisplayName("Tests queries with where clause containing nested AND.")
    void testQueryWhereNestedAnd() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE \"field\" > 0 AND (\"field1\" > 0 AND \"field2\" > 6)",
                        DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
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
                                + ": {\"$cond\": [{\"$and\": [{\"$eq\": [true, "
                                + "{\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, "
                                + "{\"$gt\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$gt\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, true, "
                                + "{\"$cond\": [{\"$or\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$gt\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$gt\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, "
                                + "{\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, false, null]}]}}}"),
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
    @DisplayName("Tests queries with where clause containing nested combined NOT, OR, and AND.")
    void testQueryWhereNotAndOr() throws SQLException {
        final String query =
                String.format(
                        "SELECT * FROM \"%s\".\"%s\" " +
                                "WHERE ((NOT \"field\" > 0 AND \"field2\" < 10) AND (NOT \"field1\" > 0 OR \"field2\" > 6)) OR \"field2\" > 0",
                        DATABASE_NAME, COLLECTION_NAME + "_array");
        final DocumentDbMqlQueryContext result = queryMapper.get(query);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLLECTION_NAME, result.getCollectionName());
        Assertions.assertEquals(5, result.getColumnMetaData().size());
        Assertions.assertEquals(6, result.getAggregateOperations().size());
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$match\": {\"$or\": [{\"array.field\": {\"$exists\": true}}, {\"array.field1\": {\"$exists\": true}}, {\"array.field2\": {\"$exists\": true}}]}}"),
                result.getAggregateOperations().get(0));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$unwind\": {\"path\": \"$array\", \"preserveNullAndEmptyArrays\": true, \"includeArrayIndex\": \"array_index_lvl_0\"}}"),
                result.getAggregateOperations().get(1));
        Assertions.assertEquals(
                BsonDocument.parse(
                        "{\"$project\": {"
                                + "\"_id\": 1, "
                                + "\"array_index_lvl_0\": 1, "
                                + "\"array.field\": 1, "
                                + "\"array.field1\": 1, \""
                                + "array.field2\": 1, "
                                + DocumentDbFilter.BOOLEAN_FLAG_FIELD
                                + ": {\"$cond\": [{\"$or\": [{\"$eq\": [true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 10}, null]}]}, {\"$lt\": [\"$array.field2\", {\"$literal\": 10}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$or\": [{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, false, null]}]}]}]}, true, "
                                + "{\"$cond\": [{\"$or\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 10}, null]}]}, {\"$lt\": [\"$array.field2\", {\"$literal\": 10}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$or\": [{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, false, null]}]}]}]}, false, null]}]}]}, "
                                + "{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 0}]}, null]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$eq\": [true, "
                                + "{\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, {\"$gt\": [{\"$literal\": 0}, null]}]}, "
                                + "{\"$lte\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}, {\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 10}, null]}]}, {\"$lt\": [\"$array.field2\", {\"$literal\": 10}]}, null]}]}, {\"$eq\": [true, {\"$cond\": [{\"$or\": [{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, {\"$gt\": [{\"$literal\": 0}, null]}]}, "
                                + "{\"$lte\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, {\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, {\"$gt\": [{\"$literal\": 6}, null]}]}, "
                                + "{\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, true, {\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, false, null]}]}]}]}, true, "
                                + "{\"$cond\": [{\"$or\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field\", {\"$literal\": 0}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 10}, null]}]}, {\"$lt\": [\"$array.field2\", {\"$literal\": 10}]}, null]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$or\": [{\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, {\"$eq\": [true, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 6}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, true, "
                                + "{\"$cond\": [{\"$and\": [{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field1\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$lte\": [\"$array.field1\", {\"$literal\": 0}]}, null]}]}, {\"$eq\": [false, "
                                + "{\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, {\"$gt\": [{\"$literal\": 6}, null]}]}, "
                                + "{\"$gt\": [\"$array.field2\", {\"$literal\": 6}]}, null]}]}]}, false, null]}]}]}]}, false, null]}]}]}, "
                                + "{\"$eq\": [false, {\"$cond\": [{\"$and\": [{\"$gt\": [\"$array.field2\", null]}, "
                                + "{\"$gt\": [{\"$literal\": 0}, null]}]}, {\"$gt\": [\"$array.field2\", {\"$literal\": 0}]}, null]}]}]}, false, null]}]}}}"),
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
