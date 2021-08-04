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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbQueryExecutorTest extends DocumentDbFlapDoodleTest {
    private static final String DATABASE_NAME = "database";
    private static final String COLLECTION_NAME = "testCollection";
    private static final String TEST_USER = "user";
    private static final String TEST_PASSWORD = "password";
    private static final String QUERY = "SELECT COUNT(*) FROM \"database\".\"testCollection\"";
    private static final DocumentDbConnectionProperties VALID_CONNECTION_PROPERTIES =
            new DocumentDbConnectionProperties();
    private static DocumentDbStatement statement;
    private ResultSet resultSet;

    @BeforeAll
    @SuppressFBWarnings(
            value = "HARD_CODE_PASSWORD",
            justification = "Hardcoded for test purposes only")
    void initialize() throws SQLException {
        // Add a valid users to the local MongoDB instance.
        createUser(DATABASE_NAME, TEST_USER, TEST_PASSWORD);
        VALID_CONNECTION_PROPERTIES.setUser(TEST_USER);
        VALID_CONNECTION_PROPERTIES.setPassword(TEST_PASSWORD);
        VALID_CONNECTION_PROPERTIES.setDatabase(DATABASE_NAME);
        VALID_CONNECTION_PROPERTIES.setTlsEnabled("false");
        VALID_CONNECTION_PROPERTIES.setHostname("localhost:" + getMongoPort());

        prepareSimpleConsistentData(DATABASE_NAME, COLLECTION_NAME, 1, TEST_USER, TEST_PASSWORD);
        final DocumentDbConnection connection = new DocumentDbConnection(VALID_CONNECTION_PROPERTIES);
        statement =
                new DocumentDbStatement(
                        connection,
                        new MockQueryExecutor(
                                statement,
                                VALID_CONNECTION_PROPERTIES,
                                null,
                                0,
                                0));
    }

    @AfterEach
    void afterAll() throws SQLException {
        final SchemaWriter schemaWriter = SchemaStoreFactory.createWriter(VALID_CONNECTION_PROPERTIES);
        schemaWriter.remove("id");
        if (resultSet != null) {
            resultSet.close();
        }
    }

    /** Tests that canceling a query before it has been executed fails. */
    @Test
    @DisplayName("Tests canceling a query without executing first.")
    public void testCancelQueryWithoutExecute() {
        final ExecutorService cancelThread = getCancelThread();
        final Cancel cancel = launchCancelThread(0, statement, cancelThread);
        waitCancelToComplete(cancelThread);
        final SQLException exception = getCancelException(cancel);
        Assertions.assertNotNull(exception);
        Assertions.assertEquals(
                "Cannot cancel query, it is either completed or has not started.",
                exception.getMessage());
    }

    /**
     * Tests that canceling a query while it is executing succeeds and that the query execution then
     * fails because it has been canceled.
     */
    @Test
    @DisplayName("Tests canceling a query while execution is in progress.")
    public void testCancelQueryWhileExecuteInProgress() {
        // Wait 100 milliseconds before attempting to cancel.
        final ExecutorService cancelThread = getCancelThread();
        final Cancel cancel = launchCancelThread(100, statement, cancelThread);

        // Check that query was canceled and cancel thread did not throw exception.
        Assertions.assertEquals(
                "Query has been canceled.",
                Assertions.assertThrows(SQLException.class, () -> resultSet = statement.executeQuery(QUERY))
                        .getMessage());
        waitCancelToComplete(cancelThread);
        Assertions.assertNull(cancel.getException(), () -> cancel.getException().getMessage());
    }

    /** Tests that canceling a query from two different threads. */
    @Test
    @DisplayName("Tests canceling a query from 2 different threads simultaneously.")
    public void testCancelQueryFromTwoThreads() {
        // Let 2 threads both wait for 100 milliseconds before attempting to cancel.
        final ExecutorService cancelThread1 = getCancelThread();
        final ExecutorService cancelThread2 = getCancelThread();
        final Cancel cancel1 = launchCancelThread(500, statement, cancelThread1);
        final Cancel cancel2 = launchCancelThread(100, statement, cancelThread2);

        // Check that query was canceled.
        Assertions.assertEquals(
                "Query has been canceled.",
                Assertions.assertThrows(SQLException.class,
                                () -> resultSet = statement.executeQuery(QUERY))
                        .getMessage());
        waitCancelToComplete(cancelThread1);
        waitCancelToComplete(cancelThread2);

        // Check that 1 of the threads succeeded while other threw an exception.
        final SQLException exception1 = cancel1.getException();
        if (exception1 == null) {
            Assertions.assertNotNull(cancel2.getException());
        } else {
            Assertions.assertNotNull(exception1);
        }
    }

    /** Tests that canceling a query after execution has already completed fails. */
    @Test
    @DisplayName("Tests canceling query after execution already completes.")
    public void testCancelQueryAfterExecuteComplete() {
        // Execute query.
        Assertions.assertDoesNotThrow(() -> statement.execute(QUERY));

        // Launch cancel after execution has already completed.
        final ExecutorService cancelThread = getCancelThread();
        final Cancel cancel = launchCancelThread(0, statement, cancelThread);
        waitCancelToComplete(cancelThread);
        final SQLException exception = getCancelException(cancel);
        Assertions.assertNotNull(exception);
        Assertions.assertEquals(
                "Cannot cancel query, it is either completed or has not started.",
                exception.getMessage());
    }

    /** Tests canceling a query after it has already been canceled. */
    @Test
    @DisplayName("Tests canceling a query after it has already been canceled.")
    public void testCancelQueryTwice() {
        // Wait 100 milliseconds before attempting to cancel.
        final ExecutorService cancelThread1 = getCancelThread();
        final Cancel cancel1 = launchCancelThread(100, statement, cancelThread1);

        // Check that query was canceled and cancel thread did not throw exception.
        Assertions.assertEquals(
                "Query has been canceled.",
                Assertions.assertThrows(SQLException.class, () -> resultSet = statement.executeQuery(QUERY))
                        .getMessage());
        waitCancelToComplete(cancelThread1);
        Assertions.assertNull(cancel1.getException(), () -> cancel1.getException().getMessage());

        // Attempt to cancel again.
        final ExecutorService cancelThread2 = getCancelThread();
        final Cancel cancel2 = launchCancelThread(1, statement, cancelThread2);
        waitCancelToComplete(cancelThread2);
        final SQLException exception = getCancelException(cancel2);
        Assertions.assertNotNull(exception);
        Assertions.assertEquals(
                "Cannot cancel query, it is either completed or has not started.",
                exception.getMessage());
    }

    private ExecutorService getCancelThread() {
        return Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("cancelThread").setDaemon(true).build());
    }

    private Cancel launchCancelThread(
            final int waitTime, final Statement statement, final ExecutorService cancelThread) {
        final Cancel cancel1 = new Cancel(statement, waitTime);
        cancelThread.execute(cancel1);
        return cancel1;
    }

    private SQLException getCancelException(final Cancel cancel) {
        return cancel.getException();
    }

    @SneakyThrows
    private void waitCancelToComplete(final ExecutorService cancelThread) {
        cancelThread.awaitTermination(10000, TimeUnit.MILLISECONDS);
    }

    /** Class to cancel query in a separate thread. */
    private static class Cancel implements Runnable {
        private final Statement statement;
        private final int waitTime;
        private SQLException exception;

        Cancel(final Statement statement, final int waitTime) {
            this.statement = statement;
            this.waitTime = waitTime;
        }

        @SneakyThrows
        @Override
        public void run() {
            try {
                Thread.sleep(waitTime);
                statement.cancel();
            } catch (final SQLException e) {
                exception = e;
            }
        }

        /**
         * Function to get exception if the run call generated one.
         */
        public SQLException getException()  {
            return exception;
        }
    }

    /**
     * Identical to actual DocumentDbQueryExecutor but overrides runQuery, so we can simulate a
     * long-running query with find instead.
     */
    private static class MockQueryExecutor extends DocumentDbQueryExecutor {
        MockQueryExecutor(
                final Statement statement,
                final DocumentDbConnectionProperties connectionProperties,
                final DocumentDbQueryMappingService queryMapper,
                final int queryTimeoutSecs,
                final int maxFetchSize) {
            super(statement, connectionProperties, queryMapper, queryTimeoutSecs, maxFetchSize);
        }

        @Override
        protected java.sql.ResultSet runQuery(final String sql) throws SQLException {
            final MongoClientSettings settings = VALID_CONNECTION_PROPERTIES.buildMongoClientSettings();
            final MongoClient client = MongoClients.create(settings);
            final MongoDatabase database =
                    client.getDatabase(VALID_CONNECTION_PROPERTIES.getDatabase());
            final MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            // We use the $where operator to sleep for 5000 milliseconds. This operator
            // can only be used with find().
            final Document whereDoc =
                    new Document("$where", "function(){ return sleep(5000) || true;}");
            final FindIterable<Document> iterable = collection.find(whereDoc).comment(getQueryId());
            final MongoCursor<Document> iterator = iterable.iterator();
            final JdbcColumnMetaData column =
                    JdbcColumnMetaData.builder().columnLabel("EXPR$0").ordinal(0).build();
            return new DocumentDbResultSet(
                    statement, iterator, ImmutableList.of(column), ImmutableList.of("EXPR$0"));
        }
    }
}
