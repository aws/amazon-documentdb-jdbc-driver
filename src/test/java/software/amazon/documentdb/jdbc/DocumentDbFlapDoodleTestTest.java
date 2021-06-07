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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;

/**
 * Test that the base class can prepare test data.
 */
@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbFlapDoodleTestTest extends DocumentDbFlapDoodleTest {

    /**
     * Tests that we can create a user and authenticate with it.
     */
    @Test
    protected void testAuthenticatedUser() {
        final String databaseName = "testAuthenticatedDatabase";
        final String authDatabase = "admin";
        final String username = "username";
        final String password = "password";

        createUser(databaseName, username, password);
        try (MongoClient client = createMongoClient(authDatabase, username, password)) {
            final MongoDatabase database = client.getDatabase(databaseName);
            database.runCommand(new Document("ping", 1));
        }

    }

    /**
     * Tests that we can prepare data and retrieve it back again.
     */
    @Test
    protected void testPrepareData()  {
        final String database = "testPrepareDataDatabase";
        final String collection = "testPrepareDataCollection";
        final int expectedRecordCount = 10;
        prepareSimpleConsistentData(database, collection, expectedRecordCount, ADMIN_USERNAME, ADMIN_PASSWORD);
        try (MongoClient client = createMongoClient(ADMIN_DATABASE, ADMIN_USERNAME, ADMIN_PASSWORD)) {
            final MongoDatabase mongoDatabase = client.getDatabase(database);
            final MongoCollection<BsonDocument> mongoCollection = mongoDatabase
                    .getCollection(collection, BsonDocument.class);
            final MongoCursor<BsonDocument> cursor = mongoCollection.find().cursor();
            int actualRecordCount = 0;
            while (cursor.hasNext()) {
                final BsonDocument document = cursor.next();
                actualRecordCount++;
                Assertions.assertEquals(Integer.MAX_VALUE, document.getInt32("fieldInt").getValue());
            }
            Assertions.assertEquals(expectedRecordCount, actualRecordCount);
        }
    }
}
