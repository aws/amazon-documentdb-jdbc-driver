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

package software.amazon.documentdb.jdbc.metadata;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironmentFactory;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.sql.SQLException;
import java.util.UUID;
import java.util.function.Consumer;

class DocumentDbMetadataTest {

    // Need to start and stop the test environment between tests to clear the collections.
    @BeforeEach
    void beforeEach() throws Exception {
        // Start the test environment.
        final DocumentDbTestEnvironment testEnvironment = DocumentDbTestEnvironmentFactory
                .getMongoDb40Environment();
        testEnvironment.start();
    }

    @AfterEach
    void afterEach() throws Exception {
        // Stop the test environment.
        DocumentDbTestEnvironmentFactory.getMongoDb40Environment().stop();
    }

    @DisplayName("Test to get database metadata for initial or latest version.")
    @Test
    void testGetInitialWithRefresh() throws SQLException, DocumentDbSchemaException {
        final DocumentDbTestEnvironment testEnvironment = DocumentDbTestEnvironmentFactory
                .getMongoDb40Environment();

        final String schemaName = UUID.randomUUID().toString();
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(testEnvironment.getJdbcConnectionString());
        final DocumentDbDatabaseSchemaMetadata databaseMetadata0 = DocumentDbDatabaseSchemaMetadata
                .get(schemaName, properties);
        Assertions.assertEquals(1, databaseMetadata0.getSchemaVersion());
        Assertions.assertEquals(0, databaseMetadata0.getTableSchemaMap().size());

        // Prepare some data.
        final String collectionName = testEnvironment.newCollectionName(true);
        prepareTestData(testEnvironment, collectionName, collection -> testEnvironment
                .prepareSimpleConsistentData(collection, 10));

        // Even though we've added data, we're not refreshing, so expecting 0.
        final DocumentDbDatabaseSchemaMetadata databaseMetadata00 = DocumentDbDatabaseSchemaMetadata
                .get(schemaName, properties, false);
        Assertions.assertEquals(0, databaseMetadata00.getTableSchemaMap().size());
        Assertions.assertEquals(1, databaseMetadata0.getSchemaVersion());
        Assertions.assertEquals(0, databaseMetadata0.getTableSchemaMap().size());

        // Now use the "refreshAll=true" flag to re-read the collection(s).
        final DocumentDbDatabaseSchemaMetadata databaseMetadata1 = DocumentDbDatabaseSchemaMetadata
                .get(schemaName, properties, true);

        Assertions.assertEquals(1,
                databaseMetadata1.getTableSchemaMap().size());
        Assertions.assertEquals(2, databaseMetadata1.getSchemaVersion());
        final DocumentDbSchemaTable metadataTable = databaseMetadata1.getTableSchemaMap().get(collectionName);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(13, metadataTable.getColumnMap().size());

        // Without a refresh we'll get the same metadata.
        final DocumentDbDatabaseSchemaMetadata databaseMetadata2 = DocumentDbDatabaseSchemaMetadata
                .get(schemaName, properties);
        // This is exactly the same as it is cached.
        Assertions.assertEquals(databaseMetadata1, databaseMetadata2);
        Assertions.assertEquals(2, databaseMetadata2.getSchemaVersion());
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties);
        schemaWriter.remove(schemaName);
    }

    @DisplayName("Test to get database metadata for specific version.")
    @Test
    void testGetSpecific() throws SQLException, DocumentDbSchemaException {
        final DocumentDbTestEnvironment testEnvironment = DocumentDbTestEnvironmentFactory
                .getMongoDb40Environment();

        final String schemaName = UUID.randomUUID().toString();
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(testEnvironment.getJdbcConnectionString());
        final DocumentDbDatabaseSchemaMetadata databaseMetadata0 = DocumentDbDatabaseSchemaMetadata
                .get(schemaName, properties);
        Assertions.assertEquals(0,
                databaseMetadata0.getTableSchemaMap().size());
        Assertions.assertEquals(1, databaseMetadata0.getSchemaVersion());

        // Prepare some data.
        final String collectionName = testEnvironment.newCollectionName(true);
        prepareTestData(testEnvironment, collectionName, collection -> testEnvironment
                .prepareSimpleConsistentData(collection, 10));

        // Now use the "refreshAll=true" flag to re-read the collection(s).
        final DocumentDbDatabaseSchemaMetadata databaseMetadata1 = DocumentDbDatabaseSchemaMetadata
                .get(schemaName, properties, true);

        Assertions.assertEquals(1,
                databaseMetadata1.getTableSchemaMap().size());
        Assertions.assertEquals(2, databaseMetadata1.getSchemaVersion());
        final DocumentDbSchemaTable metadataTable = databaseMetadata1.getTableSchemaMap().get(collectionName);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(13, metadataTable.getColumnMap().size());

        final DocumentDbDatabaseSchemaMetadata databaseMetadata2 = DocumentDbDatabaseSchemaMetadata
                .get(properties, schemaName, databaseMetadata1.getSchemaVersion());
        Assertions.assertEquals(databaseMetadata1, databaseMetadata2);

        // Check that specifying an unknown version results in no associated metadata.
        final DocumentDbDatabaseSchemaMetadata databaseMetadata3 = DocumentDbDatabaseSchemaMetadata
                .get(properties, schemaName, databaseMetadata1.getSchemaVersion() + 1);
        Assertions.assertNull(databaseMetadata3);
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties);
        schemaWriter.remove(schemaName);
    }

    private static void prepareTestData(
            final DocumentDbTestEnvironment testEnvironment,
            final String collectionName,
            final Consumer<MongoCollection<BsonDocument>> dataPreparer) throws SQLException {
        try (MongoClient client = testEnvironment.createMongoClient()) {
            final MongoDatabase database = client.getDatabase(testEnvironment.getDatabaseName());
            final MongoCollection<BsonDocument> collection = database.getCollection(collectionName, BsonDocument.class);
            dataPreparer.accept(collection);
        }
    }
}
