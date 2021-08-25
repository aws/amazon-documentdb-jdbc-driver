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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironmentFactory;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Stream;

import static software.amazon.documentdb.jdbc.DocumentDbMetadataScanMethod.RANDOM;

class DocumentDbStatementTest {

    private DocumentDbTestEnvironment testEnvironment;

    @BeforeAll
    static void setup() throws Exception {
        // Start the test environments.
        for (DocumentDbTestEnvironment testEnvironment :
                DocumentDbTestEnvironmentFactory.getConfiguredEnvironments()) {
            testEnvironment.start();
        }
    }

    @AfterEach
    void afterEach() throws Exception {
        if (testEnvironment != null) {
            final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                    .getPropertiesFromConnectionString(testEnvironment.getJdbcConnectionString());
            try (SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(properties, null)) {
                schemaWriter.remove(DocumentDbSchema.DEFAULT_SCHEMA_NAME);
            }
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

    protected static Stream<DocumentDbTestEnvironment> getTestEnvironments() {
        return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream();
    }

    protected static Stream<Arguments> getTestEnvironmentsForScanMethods() {
        return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream()
                .flatMap(
                        env ->
                                Arrays.stream(DocumentDbMetadataScanMethod.values())
                                        .flatMap(method -> Stream.of(Arguments.arguments(env, method))));
    }

    protected void setTestEnvironment(final DocumentDbTestEnvironment testEnvironment) {
        this.testEnvironment = testEnvironment;
    }

    protected void insertBsonDocuments(final String collection, final BsonDocument[] documents)
            throws SQLException {
        this.testEnvironment.insertBsonDocuments(collection, documents);
    }

    protected String getDatabaseName() {
        return this.testEnvironment.getDatabaseName();
    }

    protected DocumentDbStatement getDocumentDbStatement(
            final Connection connection) throws SQLException {
        final DocumentDbStatement statement = (DocumentDbStatement) connection.createStatement();
        Assertions.assertNotNull(statement);
        return statement;
    }

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcConnectionString(RANDOM));
    }

    protected Connection getConnection(final DocumentDbMetadataScanMethod scanMethod) throws SQLException {
        return DriverManager.getConnection(getJdbcConnectionString(scanMethod));
    }

    protected String getJdbcConnectionString(final DocumentDbMetadataScanMethod scanMethod) {
        return this.testEnvironment.getJdbcConnectionString(scanMethod);
    }

    /**
     * Prepares data for a given database and collection.
     * @param collectionName - the name of the collection to insert data into.
     * @param recordCount - the number of records to insert data into.
     */
    protected void prepareSimpleConsistentData(
            final String collectionName,
            final int recordCount) throws SQLException {
        try (MongoClient client = this.testEnvironment.createMongoClient()) {
            final MongoDatabase database = client.getDatabase(getDatabaseName());
            final MongoCollection<BsonDocument> collection = database.getCollection(collectionName, BsonDocument.class);
            this.testEnvironment.prepareSimpleConsistentData(collection, recordCount);
        }
    }
}
