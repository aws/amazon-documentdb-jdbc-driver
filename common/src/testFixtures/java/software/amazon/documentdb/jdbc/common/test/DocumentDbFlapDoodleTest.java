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

package software.amazon.documentdb.jdbc.common.test;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

/**
 * Base class for DocumentDb FlapDoodle tests
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DocumentDbFlapDoodleTest extends DocumentDbTest {
    protected static final String ADMIN_DATABASE = "admin";
    protected static final String ADMIN_USERNAME = "admin";
    protected static final String ADMIN_PASSWORD = "admin";

    @BeforeAll
    void init(final Integer mongoPort) {
        setMongoPort(mongoPort);
    }

    /**
     * Creates a user to the admin database with dbOwner role on another database.
     * @param databaseName the name of database to grant access for the user.
     * @param username the user name to create.
     * @param password the password for the user.
     * @throws IOException if unable to start the mongo shell process.
     */
    protected static void createUser(
            final String databaseName,
            final String username,
            final String password) {

        try (MongoClient client = createMongoClient(ADMIN_DATABASE, ADMIN_USERNAME, ADMIN_PASSWORD)) {
            final MongoDatabase db = client.getDatabase(ADMIN_DATABASE);
            final BasicDBObject createUserCommand = new BasicDBObject("createUser", username)
                    .append("pwd", password)
                    .append("roles",
                    Collections.singletonList(new BasicDBObject("role", "dbOwner").append("db", databaseName)));
            db.runCommand(createUserCommand);
        }
    }

    /**
     * Prepares data for a given database and collection.
     * @param databaseName - the name of the database to insert data into.
     * @param collectionName - the name of the collection to insert data into.
     * @param recordCount - the number of records to insert data into.
     */
    protected static void prepareSimpleConsistentData(
            final String databaseName,
            final String collectionName,
            final int recordCount,
            final String username,
            final String password) {

        try (MongoClient client = createMongoClient(ADMIN_DATABASE, username, password)) {
            final MongoDatabase database = client.getDatabase(databaseName);
            final MongoCollection<BsonDocument> collection = database
                    .getCollection(collectionName, BsonDocument.class);

            for (int count = 0; count < recordCount; count++) {
                // Types not supported in DocumentDB
                //BsonRegularExpression
                //BsonJavaScript
                //BsonJavaScriptWithScope
                //BsonDecimal128
                final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
                final BsonDocument document = new BsonDocument()
                        .append("_id", new BsonObjectId())
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
                        .append("fieldBinary", new BsonBinary(new byte[] { 0, 1, 2 }))
                        .append("fieldDecimal128", new BsonDecimal128(Decimal128.POSITIVE_INFINITY));

                final InsertOneResult result = collection.insertOne(document);
                Assertions.assertEquals(count + 1, collection.countDocuments());
                Assertions.assertEquals(document.getObjectId("_id"), result.getInsertedId());
            }
        }
    }
}
