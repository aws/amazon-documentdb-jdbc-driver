/*
 * Copyright <2021> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb;

import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.IOException;

/**
 * Test that the base class can start and stop MongoDB instances and prepare test data.
 */
public class DocumentDbTestTest extends DocumentDbTest {

    /**
     * Ensures any started instance is stopped.
     */
    @AfterAll
    protected static void cleanup() {
        stopMongoDbInstance();
    }

    /**
     * Tests that mongod can be started and stop using default parameters.
     * @throws IOException if unable to start the process.
     */
    @Test
    protected void testStartMongoDbDefault() throws IOException {
        Assertions.assertFalse(isMongoDbProcessRunning());
        Assertions.assertTrue(startMongoDbInstance());
        Assertions.assertTrue(isMongoDbProcessRunning());
        Assertions.assertTrue(stopMongoDbInstance());
        Assertions.assertFalse(isMongoDbProcessRunning());
    }

    /**
     * Tests that mongod can be started on a non-default port.
     * @throws IOException if unable to start the process.
     */
    @Test
    protected void testStartMongoDbCustomPort() throws IOException {
        Assertions.assertFalse(isMongoDbProcessRunning());
        Assertions.assertTrue(startMongoDbInstance(27018));
        Assertions.assertTrue(isMongoDbProcessRunning());
        Assertions.assertEquals(27018, getMongoPort());
        Assertions.assertTrue(stopMongoDbInstance());
        Assertions.assertFalse(isMongoDbProcessRunning());
    }

    /**
     * Tests that mongod can be started with -auth flag.
     * @throws IOException if unable to start the process.
     */
    @Test
    protected void testStartMongoDbCustomArgs() throws IOException {
        Assertions.assertFalse(isMongoDbProcessRunning());
        Assertions.assertTrue(startMongoDbInstance(true));
        Assertions.assertTrue(isMongoDbProcessRunning());
        Assertions.assertTrue(stopMongoDbInstance());
        Assertions.assertFalse(isMongoDbProcessRunning());
    }

    /**
     * Tests that without -auth flag, can connect without authentication.
     * @throws IOException if unable to start mongod.
     */
    @Test
    protected void testUnauthenticatedUser() throws IOException {

        final String databaseName = "testDatabase";

        try {
            Assertions.assertTrue(startMongoDbInstance());
            Assertions.assertTrue(isMongoDbProcessRunning());

            try (MongoClient client = createMongoClient()) {
                final MongoDatabase database = client.getDatabase(databaseName);
                database.runCommand(new Document("ping", 1));
            }
        } finally {
            Assertions.assertTrue(stopMongoDbInstance());
        }

    }

    /**
     * Tests that starting with -auth option and adding user is handled
     * @throws IOException if unable to start mongod.
     */
    @Test
    protected void testAuthenticatedUser() throws IOException {

        final String databaseName = "testAuthenticatedDatabase";
        final String authDatabase = "admin";
        final String username = "username";
        final String password = "password";

        try {
            Assertions.assertTrue(startMongoDbInstance(true));
            Assertions.assertTrue(isMongoDbProcessRunning());
            createUser(databaseName, username, password);

            try (MongoClient client = createMongoClient(authDatabase, username, password)) {
                final MongoDatabase database = client.getDatabase(databaseName);
                database.runCommand(new Document("ping", 1));
            }
        } finally {
            Assertions.assertTrue(stopMongoDbInstance());
        }

    }

    /**
     * Tests that starting with -auth flag but do not create user for database.
     * @throws IOException if unable to start mongod.
     */
    @Test
    protected void testNoAuthenticatedUser() throws IOException {
        final String databaseName = "testDatabaseNoAuthenticatedUsers";
        final String authDatabase = "admin";
        final String username = "username";
        final String password = "password";

        try {
            Assertions.assertTrue(startMongoDbInstance(true));
            Assertions.assertTrue(isMongoDbProcessRunning());
            // No created user for the database

            try (MongoClient client = createMongoClient(authDatabase, username, password)) {
                final MongoDatabase database = client.getDatabase(databaseName);
                Assertions.assertThrows(
                        MongoSecurityException.class,
                        () -> database.runCommand(new Document("ping", 1)));
            }
        } finally {
            Assertions.assertTrue(stopMongoDbInstance());
        }
    }

    /**
     * Tests that we can prepare data and retrieve it back again.
     * @throws IOException if unable to start a process.
     */
    @Test
    protected void testPrepareData() throws IOException {
        final String database = "testPrepareDataDatabase";
        final String collection = "testPrepareDataCollection";
        final int expectedRecordCount = 10;
        try {
            Assertions.assertTrue(startMongoDbInstance());
            prepareSimpleConsistentData(database, collection, expectedRecordCount);
            try (MongoClient client = createMongoClient()) {
                final MongoDatabase mongoDatabase = client.getDatabase(database);
                final MongoCollection<BsonDocument> mongoCollection = mongoDatabase
                        .getCollection(collection, BsonDocument.class);
                final MongoCursor<BsonDocument> cursor = mongoCollection.find().cursor();
                int actualRecordCount = 0;
                while (cursor.hasNext()) {
                    final BsonDocument document = cursor.next();
                    actualRecordCount++;
                    Assertions.assertEquals(Integer.MAX_VALUE,
                            document.getInt32("fieldInt").getValue());
                }
                Assertions.assertEquals(expectedRecordCount, actualRecordCount);
            }
        } finally {
            Assertions.assertTrue(stopMongoDbInstance());
        }
    }
}
