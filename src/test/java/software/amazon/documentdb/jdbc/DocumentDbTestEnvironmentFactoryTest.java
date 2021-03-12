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

package software.amazon.documentdb.jdbc;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironmentFactory;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Stream;

class DocumentDbTestEnvironmentFactoryTest {
    @BeforeAll
    static void setup() throws Exception {
        // Start the test environments.
        for (DocumentDbTestEnvironment testEnvironment :
                DocumentDbTestEnvironmentFactory.getConfiguredEnvironments()) {
            testEnvironment.start();
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        // Stop the test environments.
        for (DocumentDbTestEnvironment testEnvironment :
                DocumentDbTestEnvironmentFactory.getConfiguredEnvironments()) {
            testEnvironment.stop();
        }
    }

    private static Stream<DocumentDbTestEnvironment> getTestEnvironments() {
        return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream();
    }

    @DisplayName("Tests connectivity of the MongoClient returned from each test environment.")
    @ParameterizedTest(name = "testEnvironmentClientConnectivity - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testEnvironmentClientConnectivity(final DocumentDbTestEnvironment testEnvironment) {
        try (MongoClient client = testEnvironment.createMongoClient()) {
            final MongoDatabase database = client.getDatabase(testEnvironment.getDatabaseName());
            final Document document = database.runCommand(new Document("ping", 1));
            Assertions.assertEquals(1.0, document.getDouble("ok"));
        }
    }

    @DisplayName("Tests connection string from each test environment.")
    @ParameterizedTest(name = "testEnvironmentConnectionString - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testEnvironmentConnectionString(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        final String connectionString = testEnvironment.getJdbcConnectionString();

        try (Connection connection = DriverManager.getConnection(connectionString, new Properties())) {
            Assertions.assertTrue(connection instanceof DocumentDbConnection);
            final DatabaseMetaData metaData = connection.getMetaData();
            Assertions.assertTrue(metaData instanceof DocumentDbDatabaseMetadata);
            final ResultSet schemas = metaData.getSchemas();
            Assertions.assertNotNull(schemas);
            Assertions.assertTrue(schemas.next());
        }
    }

    @DisplayName("Tests preparing simple consistent data from each test environment.")
    @ParameterizedTest(name = "testPrepareSimpleConsistentData - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testPrepareSimpleConsistentData(final DocumentDbTestEnvironment testEnvironment) {
        final String collectionName;
        final int recordCount = 10;
        try (MongoClient client = testEnvironment.createMongoClient()) {
            final MongoDatabase database = client.getDatabase(testEnvironment.getDatabaseName());
            collectionName = testEnvironment.newCollectionName(true);
            final MongoCollection<BsonDocument> collection = database
                    .getCollection(collectionName, BsonDocument.class);
            testEnvironment.prepareSimpleConsistentData(collection, recordCount);
        }
        try (MongoClient client = testEnvironment.createMongoClient()) {
            final MongoDatabase database = client.getDatabase(testEnvironment.getDatabaseName());
            final MongoCollection<BsonDocument> collection = database
                    .getCollection(collectionName, BsonDocument.class);
            Assertions.assertEquals(recordCount, collection.countDocuments());
        }
    }
}
