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

import java.sql.SQLException;
import java.util.Properties;
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
    void testGetInitialWithRefresh() throws SQLException {
        final DocumentDbTestEnvironment testEnvironment = DocumentDbTestEnvironmentFactory
                .getMongoDb40Environment();

        final String id = UUID.randomUUID().toString();
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(
                        new Properties(),
                        testEnvironment.getJdbcConnectionString(),
                        "jdbc:documentdb:");
        final DocumentDbDatabaseSchemaMetadata databaseMetadata0 = DocumentDbDatabaseSchemaMetadata
                .get(id, properties);
        Assertions.assertEquals(0,
                databaseMetadata0.getCollectionMetadataMap().keySet().stream().count());
        Assertions.assertEquals(1, databaseMetadata0.getVersion());

        // Prepare some data.
        final String collectionName = testEnvironment.newCollectionName(true);
        prepareTestData(testEnvironment, collectionName, collection -> {
            testEnvironment.prepareSimpleConsistentData(collection, 10);
        });

        // Even though we've added data, we're not refreshing, so expecting 0.
        final DocumentDbDatabaseSchemaMetadata databaseMetadata00 = DocumentDbDatabaseSchemaMetadata
                .get(id, properties, false);
        Assertions.assertEquals(0,
                databaseMetadata00.getCollectionMetadataMap().keySet().stream().count());
        Assertions.assertEquals(1, databaseMetadata0.getVersion());

        // Now use the "refreshAll=true" flag to re-read the collection(s).
        final DocumentDbDatabaseSchemaMetadata databaseMetadata1 = DocumentDbDatabaseSchemaMetadata
                .get(id, properties, true);

        Assertions.assertEquals(1,
                databaseMetadata1.getCollectionMetadataMap().keySet().stream().count());
        Assertions.assertEquals(2, databaseMetadata1.getVersion());
        Assertions.assertTrue(databaseMetadata1.getCollectionMetadataMap()
                .containsKey(collectionName));
        final DocumentDbCollectionMetadata collectionMetadata = databaseMetadata1.getCollectionMetadataMap()
                .get(collectionName);
        Assertions.assertNotNull(collectionMetadata);
        final DocumentDbMetadataTable metadataTable = collectionMetadata.getTables().get(collectionName);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(13, metadataTable.getColumns().size());

        // Without a refresh we'll get the same metadata.
        final DocumentDbDatabaseSchemaMetadata databaseMetadata2 = DocumentDbDatabaseSchemaMetadata
                .get(id, properties);
        // This is exactly the same as it is cached.
        Assertions.assertEquals(databaseMetadata1, databaseMetadata2);
        Assertions.assertEquals(2, databaseMetadata2.getVersion());
    }

    @DisplayName("Test to get database metadata for specific version.")
    @Test
    void testGetSpecific() throws SQLException {
        final DocumentDbTestEnvironment testEnvironment = DocumentDbTestEnvironmentFactory
                .getMongoDb40Environment();

        final String id = UUID.randomUUID().toString();
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(
                        new Properties(),
                        testEnvironment.getJdbcConnectionString(),
                        "jdbc:documentdb:");
        final DocumentDbDatabaseSchemaMetadata databaseMetadata0 = DocumentDbDatabaseSchemaMetadata
                .get(id, properties);
        Assertions.assertEquals(0,
                databaseMetadata0.getCollectionMetadataMap().keySet().stream().count());
        Assertions.assertEquals(1, databaseMetadata0.getVersion());

        // Prepare some data.
        final String collectionName = testEnvironment.newCollectionName(true);
        prepareTestData(testEnvironment, collectionName, collection -> {
            testEnvironment.prepareSimpleConsistentData(collection, 10);
        });

        // Now use the "refreshAll=true" flag to re-read the collection(s).
        final DocumentDbDatabaseSchemaMetadata databaseMetadata1 = DocumentDbDatabaseSchemaMetadata
                .get(id, properties, true);

        Assertions.assertEquals(1,
                databaseMetadata1.getCollectionMetadataMap().keySet().stream().count());
        Assertions.assertEquals(2, databaseMetadata1.getVersion());
        Assertions.assertTrue(databaseMetadata1.getCollectionMetadataMap()
                .containsKey(collectionName));
        final DocumentDbCollectionMetadata collectionMetadata = databaseMetadata1.getCollectionMetadataMap()
                .get(collectionName);
        Assertions.assertNotNull(collectionMetadata);
        final DocumentDbMetadataTable metadataTable = collectionMetadata.getTables().get(collectionName);
        Assertions.assertNotNull(metadataTable);
        Assertions.assertEquals(13, metadataTable.getColumns().size());

        final DocumentDbDatabaseSchemaMetadata databaseMetadata2 = DocumentDbDatabaseSchemaMetadata
                .get(id, properties, databaseMetadata1.getVersion());
        Assertions.assertEquals(databaseMetadata1, databaseMetadata2);

        // Check that specifying an unknown version results in no associated metadata.
        final DocumentDbDatabaseSchemaMetadata databaseMetadata3 = DocumentDbDatabaseSchemaMetadata
                .get(id, properties, databaseMetadata1.getVersion() + 1);
        Assertions.assertNull(databaseMetadata3);
    }

    private static void prepareTestData(
            final DocumentDbTestEnvironment testEnvironment,
            final String collectionName,
            final Consumer<MongoCollection<BsonDocument>> dataPreparer) {
        try (MongoClient client = testEnvironment.createMongoClient()) {
            final MongoDatabase database = client.getDatabase(testEnvironment.getDatabaseName());
            final MongoCollection<BsonDocument> collection = database.getCollection(collectionName, BsonDocument.class);
            dataPreparer.accept(collection);
        }
    }
}
