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

package software.amazon.documentdb.jdbc.persist;

import com.mongodb.MongoException;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironmentFactory;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;
import software.amazon.documentdb.jdbc.metadata.DocumentDbTableSchemaGenerator;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getPropertiesFromConnectionString;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SCHEMA_TABLE_ID_SEPARATOR;

class DocumentDbSchemaWriterTest {
    private static final String DATABASE_NAME = "testDb";
    private DocumentDbTestEnvironment testEnvironment;

    private static Stream<DocumentDbTestEnvironment> getTestEnvironments() {
        return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream();
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        for (DocumentDbTestEnvironment testEnvironment : getTestEnvironments().collect(Collectors.toList())) {
            // Start the test environment.
            testEnvironment.start();
        }
    }

    @AfterEach
    void afterEach() throws SQLException {
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getJdbcConnectionString());
        final DocumentDbSchemaWriter schemaWriter = new DocumentDbSchemaWriter(properties, null);
        schemaWriter.remove(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
    }

    @AfterAll
    static void afterAll() throws Exception {
        for (DocumentDbTestEnvironment testEnvironment : getTestEnvironments().collect(Collectors.toList())) {
            testEnvironment.stop();
        }
    }

    @DisplayName("Tests writing the complete schema.")
    @ParameterizedTest(name = "testWriterWholeSchema - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWriterWholeSchema(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException, DocumentDbSchemaSecurityException {
        final DocumentDbConnectionProperties properties = getConnectionProperties(testEnvironment);
        final String collectionName = "testWriterWholeSchema";
        final Map<String, DocumentDbSchemaTable> metadata = getSchemaTableMap(collectionName);
        final DocumentDbSchema schema = new DocumentDbSchema(DATABASE_NAME, 1, metadata);

        final DocumentDbSchemaWriter writer = new DocumentDbSchemaWriter(properties, null);
        writer.write(schema, metadata.values());
    }

    @DisplayName("Tests updating table schema.")
    @ParameterizedTest(name = "testWriteTableSchema - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWriteTableSchema(final DocumentDbTestEnvironment testEnvironment) throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties(testEnvironment);
        final String collectionName = "testWriteTableSchema";
        final Map<String, DocumentDbSchemaTable> metadata = getSchemaTableMap(collectionName);
        final DocumentDbSchema schema = new DocumentDbSchema(DATABASE_NAME, 1, metadata);
        final String newUuid = UUID.randomUUID().toString();
        final String newSqlName = UUID.randomUUID().toString();

        try (DocumentDbSchemaWriter writer = new DocumentDbSchemaWriter(properties, null)) {
            // Write initial schema
            writer.write(schema, schema.getTableMap().values());

            // Update the schema to create a new one.
            final DocumentDbSchemaTable schemaTable = schema.getTableMap().get(collectionName);
            schemaTable.setUuid(newUuid);
            schemaTable.setSqlName(newSqlName);
            writer.update(schema, Collections.singletonList(schemaTable));
        }

        // Ensure both versions exist.
        try (DocumentDbSchemaReader reader = new DocumentDbSchemaReader(properties, null)) {
            final DocumentDbSchema schema1 = reader.read(schema.getSchemaName(), 1);
            Assertions.assertNotNull(schema1);
            Assertions.assertEquals(schema, schema1);
            Assertions.assertEquals(schema.getTableReferences().size(),
                    schema1.getTableReferences().size());
            Assertions.assertArrayEquals(
                    schema.getTableReferences().toArray(new String[0]),
                    schema1.getTableReferences().toArray(new String[0]));
            final DocumentDbSchema schema2 = reader.read(schema.getSchemaName(), 2);
            Assertions.assertNotNull(schema2);
            Assertions.assertEquals(1, schema2.getTableReferences().size());
            Assertions.assertEquals(
                    newSqlName + SCHEMA_TABLE_ID_SEPARATOR + newUuid,
                    schema2.getTableReferences().toArray(new String[0])[0]);
        }
    }

    @DisplayName("Tests failing to write schema for restricted user.")
    @ParameterizedTest(name = "testWriteSchemaRestrictedUser - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testWriteSchemaRestrictedUser(final DocumentDbTestEnvironment testEnvironment)
        throws SQLException {

        final DocumentDbConnectionProperties properties = getConnectionProperties(testEnvironment, true);
        final String collectionName = "testWriteTableSchema";
        final Map<String, DocumentDbSchemaTable> metadata = getSchemaTableMap(collectionName);
        final DocumentDbSchema schema = new DocumentDbSchema(DATABASE_NAME, 1, metadata);
        final DocumentDbSchemaWriter writer = new DocumentDbSchemaWriter(properties, null);
        final DocumentDbSchemaSecurityException exception = Assertions
                .assertThrows(DocumentDbSchemaSecurityException.class,
                        () -> writer.write(schema, schema.getTableMap().values()));
        Assertions.assertTrue(exception.getCause() instanceof MongoException);
        final MongoException mongoException = (MongoException) exception.getCause();
        Assertions.assertEquals(13, mongoException.getCode());
        Assertions.assertTrue(mongoException.getMessage().startsWith("Command failed with error 13"));
    }

    private DocumentDbConnectionProperties getConnectionProperties(
            final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        return getConnectionProperties(testEnvironment, false);
    }

    private DocumentDbConnectionProperties getConnectionProperties(
            final DocumentDbTestEnvironment testEnvironment,
            final boolean isRestrictedUser)
            throws SQLException {
        this.testEnvironment = testEnvironment;
        return getPropertiesFromConnectionString(
                isRestrictedUser
                        ? testEnvironment.getRestrictedUserConnectionString()
                        : testEnvironment.getJdbcConnectionString());
    }

    private Map<String, DocumentDbSchemaTable> getSchemaTableMap(
            final String collectionName) {
        final List<BsonDocument> documentList = new ArrayList<>();
        for (int count = 0; count < 3; count++) {
            final Instant dateTime = Instant.parse("2020-01-01T00:00:00.00Z");
            final BsonDocument document = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("fieldDecimal128", new BsonDecimal128(Decimal128.parse(String.valueOf(Double.MAX_VALUE))))
                    .append("fieldDouble", new BsonDouble(Double.MAX_VALUE))
                    .append("fieldString", new BsonString("新年快乐"))
                    .append("fieldObjectId", new BsonObjectId())
                    .append("fieldBoolean", new BsonBoolean(true))
                    .append("fieldDate", new BsonDateTime(dateTime.toEpochMilli()))
                    .append("fieldInt", new BsonInt32(Integer.MAX_VALUE))
                    .append("fieldLong", new BsonInt64(Long.MAX_VALUE))
                    .append("fieldMaxKey", new BsonMaxKey())
                    .append("fieldMinKey", new BsonMinKey())
                    .append("fieldNull", new BsonNull())
                    .append("fieldBinary", new BsonBinary(new byte[]{0, 1, 2}))
                    .append("fieldTimestamp",
                            new BsonTimestamp((int) TimeUnit.MILLISECONDS.toSeconds(dateTime.toEpochMilli()), 0));
            Assertions.assertTrue(documentList.add(document));
        }

        // Discover the collection metadata.
        return DocumentDbTableSchemaGenerator.generate(
                collectionName, documentList.iterator());
    }
}
