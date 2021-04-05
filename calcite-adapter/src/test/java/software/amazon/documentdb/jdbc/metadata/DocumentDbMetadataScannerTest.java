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
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.DocumentDbMetadataScanMethod;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbMetadataScannerTest extends DocumentDbFlapDoodleTest {

    private static final String USER = "user";
    private static final String PASS = "password";
    private static final String DATABASE = "testDb";
    private static final String HOST = "localhost";
    private static final String ADMIN = "admin";

    private DocumentDbConnectionProperties properties;

    private ArrayList<BsonDocument> documents;
    private ArrayList<String> ids;

    private MongoDatabase database;
    /**
     * Init mongodb for testing
     */
    @BeforeAll
    public void setup()  {
        createUser(DATABASE, USER, PASS);
        final MongoClient client = createMongoClient(ADMIN, USER, PASS);
        database = client.getDatabase(DATABASE);
    }

    /**
     * Resets the properties, documents, and ids.
     */
    @BeforeEach
    public void closeAndResetProperties() {
        properties = new DocumentDbConnectionProperties();
        properties.setUser(USER);
        properties.setPassword(PASS);
        properties.setDatabase(DATABASE);
        properties.setTlsEnabled("false");
        properties.setHostname(HOST + ":" + getMongoPort());
        documents = new ArrayList<>();
        ids = new ArrayList<>();

    }

    /**
     * Test for basic default (random) scan.
     */
    @Test
    public void testGetIteratorBasic() throws SQLException {
        addSimpleDataToDatabase(3, "testGetIteratorBasic");
        final HashSet<BsonDocument> documentSet = new HashSet<>(documents);
        Assertions.assertEquals(DocumentDbMetadataScanMethod.RANDOM, properties.getMetadataScanMethod());
        properties.setMetadataScanLimit("1");
        final MongoCollection<BsonDocument> collection = database.getCollection("testGetIteratorBasic",
                BsonDocument.class);

        final Iterator<BsonDocument> iterator = DocumentDbMetadataScanner.getIterator(properties, collection);

        Assertions.assertTrue(documentSet.contains(iterator.next()));
        Assertions.assertThrows(NoSuchElementException.class,
                iterator::next);
    }

    /**
     * Test that forward id scan works and is ordered correctly.
     */
    @Test
    public void testGetIteratorForward() throws SQLException {
        addSimpleDataToDatabase(10, "testGetIteratorForward");
        properties.setMetadataScanMethod(DocumentDbMetadataScanMethod.ID_FORWARD.getName());
        properties.setMetadataScanLimit("5");
        final MongoCollection<BsonDocument> collection = database.getCollection("testGetIteratorForward",
                BsonDocument.class);

        final Iterator<BsonDocument> iterator = DocumentDbMetadataScanner.getIterator(properties, collection);
        Collections.sort(ids);
        for (int i = 0; i < 5; i++) {
            Assertions.assertTrue(iterator.hasNext());
            final BsonDocument document = iterator.next();
            Assertions.assertEquals(ids.get(i), document.get("_id").toString());
        }
        Assertions.assertThrows(NoSuchElementException.class,
                iterator::next);
    }

    /**
     * Tests that random scanning produces different iterators each time.
     *
     * NOTE: In theory could fail incorrectly, as there is a slim (<0.0001%) chance that the randomization
     *       will result in identical iterators.
     */
    @Test
    public void testGetIteratorRandom() throws SQLException {
        addSimpleDataToDatabase(105, "testGetIteratorRandom");
        properties.setMetadataScanMethod(DocumentDbMetadataScanMethod.RANDOM.getName());
        properties.setMetadataScanLimit("5");
        final MongoCollection<BsonDocument> collection = database.getCollection("testGetIteratorRandom",
                BsonDocument.class);

        final Iterator<BsonDocument> iterator = DocumentDbMetadataScanner.getIterator(properties, collection);
        final Iterator<BsonDocument> iteratorRepeat = DocumentDbMetadataScanner.getIterator(properties, collection);
        final ArrayList<BsonDocument> firstDocumentList = new ArrayList<>();
        final ArrayList<BsonDocument> secondDocumentList = new ArrayList<>();
        while (iterator.hasNext()) {
            firstDocumentList.add(iterator.next());
            secondDocumentList.add(iteratorRepeat.next());
        }
        Assertions.assertNotEquals(firstDocumentList, secondDocumentList);
    }


    /**
     * Test for all option.
     */
    @Test
    public void testGetIteratorAll() throws SQLException {
        addSimpleDataToDatabase(3, "testGetIteratorAll");
        final HashSet<BsonDocument> documentSet = new HashSet<>(documents);
        properties.setMetadataScanMethod(DocumentDbMetadataScanMethod.ALL.getName());
        final MongoCollection<BsonDocument> collection = database.getCollection("testGetIteratorAll",
                BsonDocument.class);

        final Iterator<BsonDocument> iterator = DocumentDbMetadataScanner.getIterator(properties, collection);
        for (int n = 0; n < 3; n++) {
            Assertions.assertTrue(documentSet.contains(iterator.next()));
        }
        Assertions.assertThrows(NoSuchElementException.class,
                iterator::next);
    }

    /**
     * Tests get iterator with reverse id order.
     */
    @Test
    public void testGetIteratorReverse() throws SQLException {
        addSimpleDataToDatabase(5, "testGetIteratorReverse");
        properties.setMetadataScanLimit("5");
        properties.setMetadataScanMethod(DocumentDbMetadataScanMethod.ID_REVERSE.getName());
        final MongoCollection<BsonDocument> collection = database.getCollection("testGetIteratorReverse",
                BsonDocument.class);
        final Iterator<BsonDocument> iterator = DocumentDbMetadataScanner.getIterator(properties, collection);
        Collections.sort(ids);
        for (int i = 4; i >= 0; i--) {
            Assertions.assertTrue(iterator.hasNext());
            final BsonDocument document = iterator.next();
            Assertions.assertEquals(ids.get(i), document.get("_id").toString());
        }
        Assertions.assertThrows(NoSuchElementException.class,
                iterator::next);
    }

    /**
     * Prepares data for a given database and collection.
     * @param recordCount - the number of records to insert data into.
     */
    protected void addSimpleDataToDatabase(final int recordCount, final String collectionName) {
        final MongoCollection<BsonDocument> collection = database
                .getCollection(collectionName, BsonDocument.class);

        for (int count = 0; count < recordCount; count++) {
            final BsonDocument document = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("count", new BsonInt32(count));
            documents.add(document);
            ids.add(document.get("_id").toString());
        }
        collection.insertMany(documents);
        Assertions.assertEquals(recordCount, collection.countDocuments());
    }
}
