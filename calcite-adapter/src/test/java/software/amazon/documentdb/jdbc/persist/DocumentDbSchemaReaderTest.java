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

import com.mongodb.MongoSecurityException;
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
import org.bson.types.Decimal128;
import org.junit.jupiter.api.AfterAll;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getPropertiesFromConnectionString;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.DEFAULT_SCHEMA_NAME;

class DocumentDbSchemaReaderTest {
    private static final String DATABASE_NAME = "testDb";
    private static final String COLLECTION_NAME = DocumentDbSchemaReaderTest.class.getSimpleName();

    private static final Map<String, DocumentDbSchemaTable> METADATA;
    private static final DocumentDbSchema SCHEMA;
    private static final String TABLE_ID;

    private static Stream<DocumentDbTestEnvironment> getTestEnvironments() {
        return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream();
    }

    static {
        final List<BsonDocument> documentList = new ArrayList<>();
        for (int count = 0; count < 3; count++) {
            final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
            final BsonDocument document = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("fieldDecimal128", new BsonDecimal128(Decimal128.POSITIVE_INFINITY))
                    .append("fieldDouble", new BsonDouble(Double.MAX_VALUE))
                    .append("fieldString", new BsonString("新年快乐"))
                    .append("fieldObjectId", new BsonObjectId())
                    .append("fieldBoolean", new BsonBoolean(true))
                    .append("fieldDate", new BsonDateTime(dateTime))
                    .append("fieldInt", new BsonInt32(Integer.MAX_VALUE))
                    .append("fieldLong", new BsonInt64(Long.MAX_VALUE))
                    .append("fieldMaxKey", new BsonMaxKey())
                    .append("fieldMinKey", new BsonMinKey())
                    .append("fieldNull", new BsonNull())
                    .append("fieldBinary", new BsonBinary(new byte[]{0, 1, 2}));
            Assertions.assertTrue(documentList.add(document));
        }

        // Discover the collection metadata.
        METADATA = DocumentDbTableSchemaGenerator.generate(COLLECTION_NAME, documentList.iterator());
        SCHEMA = new DocumentDbSchema(DATABASE_NAME, 1, METADATA);
        final DocumentDbSchemaTable schemaTable = METADATA.get(COLLECTION_NAME);
        Assertions.assertNotNull(schemaTable);
        TABLE_ID = schemaTable.getId();
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        for (DocumentDbTestEnvironment testEnvironment : getTestEnvironments()
                .collect(Collectors.toList())) {
            // Start the test environment.
            testEnvironment.start();
            final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                    testEnvironment.getJdbcConnectionString());

            final DocumentDbSchemaWriter writer = new DocumentDbSchemaWriter(properties, null);
            writer.write(SCHEMA, METADATA.values());
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        for (DocumentDbTestEnvironment testEnvironment : getTestEnvironments()
                .collect(Collectors.toList())) {
            final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                    testEnvironment.getJdbcConnectionString());
            final DocumentDbSchemaWriter schemaWriter = new DocumentDbSchemaWriter(properties, null);
            schemaWriter.remove(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
            testEnvironment.stop();
        }
    }

    @DisplayName("Test reading default schema with no options.")
    @ParameterizedTest(name = "testRead - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testRead(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        Assertions.assertNotNull(testEnvironment);
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getJdbcConnectionString());
        final DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(properties, null);
        final DocumentDbSchema schema = schemaReader.read();
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.getSchemaName());
        Assertions.assertEquals(DATABASE_NAME, schema.getSqlName());
        Assertions.assertEquals(1, schema.getSchemaVersion());
        Assertions.assertNotNull(schema.getTableReferences());
        Assertions.assertEquals(1, schema.getTableReferences().size());
    }

    @DisplayName("Test reading a schema by name.")
    @ParameterizedTest(name = "testReadWithSchema - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testReadWithSchema(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        Assertions.assertNotNull(testEnvironment);
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getJdbcConnectionString());
        final DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(properties, null);
        final DocumentDbSchema schema = schemaReader.read(DEFAULT_SCHEMA_NAME);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.getSchemaName());
        Assertions.assertEquals(DATABASE_NAME, schema.getSqlName());
        Assertions.assertEquals(1, schema.getSchemaVersion());
        Assertions.assertNotNull(schema.getTableReferences());
        Assertions.assertEquals(1, schema.getTableReferences().size());
    }

    @DisplayName("Test reading schema by name and version.")
    @ParameterizedTest(name = "testReadWithVersion - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testReadWithVersion(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        Assertions.assertNotNull(testEnvironment);
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getJdbcConnectionString());
        final DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(properties, null);
        final DocumentDbSchema schema = schemaReader.read(DEFAULT_SCHEMA_NAME, 1);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.getSchemaName());
        Assertions.assertEquals(DATABASE_NAME, schema.getSqlName());
        Assertions.assertEquals(1, schema.getSchemaVersion());
        Assertions.assertNotNull(schema.getTableReferences());
        Assertions.assertEquals(1, schema.getTableReferences().size());
    }

    @DisplayName("Test reading a specific table schema.")
    @ParameterizedTest(name = "testReadTable - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testReadTable(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        Assertions.assertNotNull(testEnvironment);
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getJdbcConnectionString());
        final DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(properties, null);
        final DocumentDbSchemaTable schemaTable = schemaReader.readTable(DEFAULT_SCHEMA_NAME, 1,
                TABLE_ID);
        Assertions.assertNotNull(schemaTable);
        Assertions.assertEquals(TABLE_ID, schemaTable.getId());
        Assertions.assertEquals(COLLECTION_NAME, schemaTable.getSqlName());
        Assertions.assertEquals(COLLECTION_NAME, schemaTable.getCollectionName());
        Assertions.assertNotNull(schemaTable.getColumnMap());
        Assertions.assertEquals(13, schemaTable.getColumnMap().size());
    }

    // Negative tests
    @DisplayName("Test reading schema with non-existent version.")
    @ParameterizedTest(name = "testReadWithNonExistentVersion - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testReadWithNonExistentVersion(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        Assertions.assertNotNull(testEnvironment);
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getJdbcConnectionString());
        final DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(properties, null);
        final DocumentDbSchema schema = schemaReader.read(DEFAULT_SCHEMA_NAME, 2);
        Assertions.assertNull(schema);
    }

    @DisplayName("Test reading schema with non-existent schema name.")
    @ParameterizedTest(name = "testReadWithNonExistentSchema - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testReadWithNonExistentSchema(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        Assertions.assertNotNull(testEnvironment);
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getJdbcConnectionString());
        final DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(properties, null);
        final DocumentDbSchema schema = schemaReader.read("unknown");
        Assertions.assertNull(schema);
    }

    @DisplayName("Test reading schema with invalid connection properties.")
    @ParameterizedTest(name = "testReadWithInvalidConnectionProperties - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testReadWithInvalidConnectionProperties(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        Assertions.assertNotNull(testEnvironment);
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getJdbcConnectionString());
        final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties(properties);
        newProperties.setUser("unknown");

        final DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(newProperties, null);
        Assertions.assertEquals("Exception authenticating "
                        + "MongoCredential{mechanism=SCRAM-SHA-1, userName='unknown', "
                        + "source='admin', password=<hidden>, mechanismProperties=<hidden>}",
                Assertions.assertThrows(MongoSecurityException.class, schemaReader::read)
                        .getMessage());
    }

    @DisplayName("Test reading schema using restricted user.")
    @ParameterizedTest(name = "testReadWithRestrictedUser - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testReadWithRestrictedUser(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        Assertions.assertNotNull(testEnvironment);
        final DocumentDbConnectionProperties properties = getPropertiesFromConnectionString(
                testEnvironment.getRestrictedUserConnectionString());

        // This will allow read of the schema collection(s)
        final DocumentDbSchemaReader schemaReader = new DocumentDbSchemaReader(properties, null);
        final DocumentDbSchema schema = schemaReader.read();
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.getSchemaName());
        Assertions.assertEquals(DATABASE_NAME, schema.getSqlName());
        Assertions.assertEquals(1, schema.getSchemaVersion());
        Assertions.assertNotNull(schema.getTableReferences());
        Assertions.assertEquals(1, schema.getTableReferences().size());

        final DocumentDbSchemaTable schemaTable = schemaReader.readTable(
                schema.getSchemaName(), schema.getSchemaVersion(),
                schema.getTableReferences().toArray(new String[]{})[0]);
        Assertions.assertNotNull(schemaTable);
        Assertions.assertEquals(TABLE_ID, schemaTable.getId());
        Assertions.assertEquals(COLLECTION_NAME, schemaTable.getSqlName());
        Assertions.assertEquals(COLLECTION_NAME, schemaTable.getCollectionName());
        Assertions.assertNotNull(schemaTable.getColumnMap());
        Assertions.assertEquals(13, schemaTable.getColumnMap().size());
    }
}
