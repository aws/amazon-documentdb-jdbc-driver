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

package software.amazon.documentdb.jdbc.query.limitations;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.query.DocumentDbMqlQueryContext;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingServiceTest;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static software.amazon.documentdb.jdbc.calcite.adapter.DocumentDbRules.quote;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
class DocumentDbSqlInjectionTest extends DocumentDbQueryMappingServiceTest {
    private static final String COLLECTION_NAME = "testCollectionInjectionTest";
    private static final String OTHER_COLLECTION_NAME = "otherTestCollectionInjectionTest";
    private DocumentDbQueryMappingService queryMapper;

    @BeforeAll
    void beforeAll() throws SQLException {
        final BsonDocument document =
                BsonDocument.parse(
                        "{ \"_id\" : \"key\", \"array\" : [ "
                                + "{ \"field\" : 1, \"field1\": \"value\" }, "
                                + "{ \"field\" : 2, \"field2\" : \"value\" , \"field3\" : { \"field4\": 3} } ]}");
        final BsonDocument otherDocument =
                BsonDocument.parse(
                        "{ \"_id\" : \"key1\", \"otherArray\" : [ { \"field\" : 1, \"field3\": \"value\" }, { \"field\" : 2, \"field3\" : \"value\" } ]}");
        insertBsonDocuments(COLLECTION_NAME, new BsonDocument[] {document});
        insertBsonDocuments(OTHER_COLLECTION_NAME, new BsonDocument[] {otherDocument});
        queryMapper = getQueryMappingService();
    }

    @Test
    void testMongoInjections() throws SQLException {
        final String primaryKeyColumnName = COLLECTION_NAME + "__id";
        final String expectedKey = "$delete";
        final String injection = String.format(
                "\"}, {$delete: {\"%1$s\", \"1\"}",
                primaryKeyColumnName);
        final String query = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" = '%6$s'",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                injection);
        final DocumentDbMqlQueryContext queryContext = queryMapper.get(query);
        Assertions.assertNotNull(queryContext);
        final List<Bson> aggregateOperations = queryContext.getAggregateOperations();
        assertKeyNotExists(expectedKey, aggregateOperations);
        assertValueExists(injection, aggregateOperations);

        final String query2 = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM (SELECT * FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" = '%6$s')",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                injection);
        final DocumentDbMqlQueryContext queryContext2 = queryMapper.get(query2);
        Assertions.assertNotNull(queryContext2);
        final List<Bson> aggregateOperations2 = queryContext2.getAggregateOperations();
        assertKeyNotExists(expectedKey, aggregateOperations2);
        assertValueExists(injection, aggregateOperations2);

        final String query3 = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" = SUBSTRING('%6$s', 1, 2000)",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                injection);
        final DocumentDbMqlQueryContext queryContext3 = queryMapper.get(query3);
        Assertions.assertNotNull(queryContext3);
        final List<Bson> aggregateOperations3 = queryContext3.getAggregateOperations();
        assertKeyNotExists(expectedKey, aggregateOperations3);
        assertValueExists(injection, aggregateOperations3);

        final String query4 = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" = CONCAT('%6$s', '')",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                injection);
        final DocumentDbMqlQueryContext queryContext4 = queryMapper.get(query4);
        Assertions.assertNotNull(queryContext4);
        final List<Bson> aggregateOperations4 = queryContext4.getAggregateOperations();
        assertKeyNotExists(expectedKey, aggregateOperations4);
        assertValueExists(injection, aggregateOperations4);

        final String query5 = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" = REVERSE('%6$s')",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                new StringBuilder(injection).reverse());
        final DocumentDbMqlQueryContext queryContext5 = queryMapper.get(query5);
        Assertions.assertNotNull(queryContext5);
        final List<Bson> aggregateOperations5 = queryContext5.getAggregateOperations();
        assertKeyNotExists(expectedKey, aggregateOperations5);
        assertValueExists(injection, aggregateOperations5);

        // Single-quotes
        final String injection6 = String.format(
                "'}, {$delete: {'%1$s', '1'}",
                primaryKeyColumnName);
        final String query6 = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" = %6$s",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                quote(injection6, '\'', Collections.singletonMap("[']", "''")));
        final DocumentDbMqlQueryContext queryContext6 = queryMapper.get(query6);
        Assertions.assertNotNull(queryContext6);
        final List<Bson> aggregateOperations6 = queryContext6.getAggregateOperations();
        assertKeyNotExists(expectedKey, aggregateOperations6);
        assertValueExists(injection6, aggregateOperations6);
    }

    @Test
    void testSqlInjections() throws SQLException {
        final String primaryKeyColumnName = COLLECTION_NAME + "__id";
        final String injection = String.format(
                "'; DELETE FROM \"%1$s\" WHERE \"%2$s\" <> '",
                COLLECTION_NAME,
                primaryKeyColumnName);
        final String query = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" = '%6$s'",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                injection);
        final Exception exception = Assertions.assertThrows(SQLException.class, () -> queryMapper.get(query));
        Assertions.assertTrue(exception.getMessage().contains("Reason: 'parse failed: Encountered \";\" at line 1"));

        final String query2 = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM (SELECT * FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" = '%6$s')",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                injection);
        final Exception exception2 = Assertions.assertThrows(SQLException.class, () -> queryMapper.get(query2));
        Assertions.assertTrue(exception2.getMessage().contains("Reason: 'parse failed: Encountered \";\" at line 1"));

        // Assume SQL application will correctly escape input strings, as below
        final String injection3 = "'--";
        final String query3 = String.format(
                "SELECT \"%1$s\", \"%2$s\", \"%3$s\" FROM \"%4$s\".\"%5$s\"" +
                        " WHERE \"%1$s\" > %6$s AND \"%1$s\" < 'detect value'",
                primaryKeyColumnName,
                "field",
                "field1",
                getDatabaseName(),
                COLLECTION_NAME + "_array",
                quote(injection3, '\'', Collections.singletonMap("[']", "''")));
        final DocumentDbMqlQueryContext queryContext3 = queryMapper.get(query3);
        Assertions.assertNotNull(queryContext3);
        final List<Bson> aggregateOperations3 = queryContext3.getAggregateOperations();
        assertValueExists("detect value", aggregateOperations3);
        assertValueExists(injection3, aggregateOperations3);
    }

    private static void assertKeyNotExists(final @NonNull String expectedKey, final List<Bson> aggregateOperations) {
        for (final Bson op : aggregateOperations) {
            final BsonDocument doc = op.toBsonDocument();
            assertKeyNotExists(expectedKey, doc);
        }
    }

    private static void assertValueExists(final @NonNull String injection, final List<Bson> aggregateOperations) {
        boolean valueExists = false;
        for (final Bson op : aggregateOperations) {
            final BsonDocument doc = op.toBsonDocument();
            valueExists = isValueExists(injection, doc);
            if (valueExists) {
                break;
            }
        }
        Assertions.assertTrue(valueExists);
    }

    private static boolean isValueExists(final @NonNull String injection, final BsonDocument doc) {
        boolean valueExists = false;
        for (final Map.Entry<String, BsonValue> entry : doc.entrySet()) {
            final BsonValue bsonValue = entry.getValue();
            if (bsonValue.isDocument()) {
                valueExists = isValueExists(injection, bsonValue.asDocument());
                if (valueExists) {
                    break;
                }
            } else if (bsonValue.isArray()) {
                valueExists = isValueExists(injection, bsonValue.asArray());
                if (valueExists) {
                    break;
                }
            } else if (bsonValue.isString()) {
                final String actualValue = bsonValue.asString().getValue();
                if (injection.equals(actualValue)) {
                    valueExists = true;
                    break;
                }
            }
        }
        return valueExists;
    }

    private static boolean isValueExists(final @NonNull String injection, final BsonArray array) {
        boolean valueExists = false;
        for (final BsonValue arrayValue : array) {
            if (arrayValue.isDocument()) {
                valueExists = isValueExists(injection, arrayValue.asDocument());
                if (valueExists) {
                    break;
                }
            } else if (arrayValue.isArray()) {
                valueExists = isValueExists(injection, arrayValue.asArray());
                if (valueExists) {
                    break;
                }
            } else if (arrayValue.isString()) {
                final String actualValue = array.asString().getValue();
                if (injection.equals(actualValue)) {
                    valueExists = true;
                    break;
                }
            }
        }
        return valueExists;
    }

    private static void assertKeyNotExists(final @NonNull String expectedKey, final BsonDocument doc) {
        for (final Map.Entry<String, BsonValue> entry : doc.entrySet()) {
            final String actualKey = entry.getKey();
            Assertions.assertNotEquals(expectedKey, actualKey);
            if (entry.getValue().isDocument()) {
                assertKeyNotExists(expectedKey, entry.getValue().asDocument());
            } else if (entry.getValue().isArray()) {
                assertKeyNotExists(expectedKey, entry.getValue().asArray());
            }
        }
    }

    private static void assertKeyNotExists(final @NonNull String expectedKey, final BsonArray array) {
        for (final BsonValue value : array) {
            if (value.isDocument()) {
                assertKeyNotExists(expectedKey, value.asDocument());
            } else if (value.isArray()) {
                assertKeyNotExists(expectedKey, value.asArray());
            }
        }
    }
}
