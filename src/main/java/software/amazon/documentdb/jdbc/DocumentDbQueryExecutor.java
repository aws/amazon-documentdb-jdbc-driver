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
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.query.DocumentDbMqlQueryContext;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * DocumentDb implementation of QueryExecution.
 */
public class DocumentDbQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbQueryExecutor.class);
    private final Object queryStateLock = new Object();
    private final Object queryIdLock = new Object();
    private int maxFetchSize;
    private final java.sql.Statement statement;
    private final int queryTimeout;
    private final DocumentDbQueryMappingService queryMapper;
    private AggregateIterable<Document> iterable = null;
    private String queryId = null;
    private QueryState queryState = QueryState.NOT_STARTED;
    private enum QueryState {
        NOT_STARTED,
        IN_PROGRESS,
        CANCELLED
    }

    /**
     * DocumentDbQueryExecutor constructor.
     */
    DocumentDbQueryExecutor(
            final java.sql.Statement statement,
            final DocumentDbQueryMappingService queryMapper,
            final int queryTimeoutSecs,
            final int maxFetchSize) {
        this.statement = statement;
        this.queryMapper = queryMapper;
        this.maxFetchSize = maxFetchSize;
        this.queryTimeout = queryTimeoutSecs;
    }

    /**
     * This function wraps query cancellation and ensures query state is kept consistent.
     * @throws SQLException
     */
    protected void cancelQuery() throws SQLException {
        synchronized (queryStateLock) {
            if (queryState.equals(QueryState.NOT_STARTED)) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANCELED);
            } else if (queryState.equals(QueryState.CANCELLED)) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANCELED);
            }

            performCancel();
            queryState = QueryState.CANCELLED;
        }
    }

    protected int getMaxFetchSize()  {
        return maxFetchSize;
    }

    /**
     * This function wraps query execution and ensures query state is kept consistent.
     *
     * @param query       Query to execute.
     * @return ResultSet Object.
     * @throws SQLException if query execution fails, or it was cancelled.
     */
    public java.sql.ResultSet executeQuery(final String query) throws SQLException {
        synchronized (queryStateLock) {
            if (queryState.equals(QueryState.IN_PROGRESS)) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.RESULT_FORWARD_ONLY);
            }
            queryState = QueryState.IN_PROGRESS;
        }

        try {
            final java.sql.ResultSet resultSet = runQuery(query);
            synchronized (queryStateLock) {
                if (queryState.equals(QueryState.CANCELLED)) {
                    resetQueryState();
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                }
                resetQueryState();
            }
            return resultSet;
        } catch (final SQLException e) {
            throw e;
        } catch (final Exception e) {
            synchronized (queryStateLock) {
                if (queryState.equals(QueryState.CANCELLED)) {
                    resetQueryState();
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                } else {
                    resetQueryState();
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED, e);
                }
            }
        }
    }

    /**
     * Function to execute query.
     * @param sql Query to execute.
     * @return java.sql.ResultSet object returned from query execution.
     */
    private java.sql.ResultSet runQuery(final String sql) throws SQLException {
        final DocumentDbMqlQueryContext queryContext = queryMapper.get(sql);

        if (!(statement.getConnection() instanceof DocumentDbConnection)) {
            throw new SQLException("Unexpected operation state.");
        }

        final DocumentDbConnection connection = (DocumentDbConnection) statement.getConnection();
        final DocumentDbConnectionProperties properties = connection.getConnectionProperties();
        final MongoClientSettings settings = properties.buildMongoClientSettings();
        try (MongoClient client = MongoClients.create(settings)) {
            final MongoDatabase database = client.getDatabase(properties.getDatabase());
            final MongoCollection<Document> collection = database
                    .getCollection(queryContext.getCollectionName());

            synchronized (queryIdLock) {
                    queryId = "abcde";
                    iterable = collection.aggregate(queryContext.getAggregateOperations()).comment(queryId);
            }

            if (getQueryTimeout() > 0) {
                iterable = iterable.maxTime(getQueryTimeout(), TimeUnit.SECONDS);
            }
            if (getMaxFetchSize() > 0) {
                iterable = iterable.batchSize(getMaxFetchSize());
            }

            final MongoCursor<Document> iterator = iterable.iterator();
            synchronized (queryIdLock) {
                queryId = null;
            }

            final ImmutableList<JdbcColumnMetaData> columnMetaData = ImmutableList
                    .copyOf(queryContext.getColumnMetaData());

            return new DocumentDbResultSet(
                    this.statement,
                    iterator,
                    columnMetaData,
                    queryContext.getPaths());
        }
    }

    /**
     * Get query execution timeout in seconds.
     * @return Query execution timeout in seconds.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }

    private void resetQueryState() {
        queryState = QueryState.NOT_STARTED;
    }

    private void performCancel() throws SQLException {
        synchronized (queryIdLock) {
            if (queryId != null ) {
                final DocumentDbConnection connection = (DocumentDbConnection) statement.getConnection();
                final DocumentDbConnectionProperties properties = connection.getConnectionProperties();
                final MongoClientSettings settings = properties.buildMongoClientSettings();
                try (MongoClient client = MongoClients.create(settings)) {
                    final MongoDatabase database = client.getDatabase(properties.getDatabase());

                    // Find the opId to kill.
                    final Document currentOp =
                            database.runCommand(
                                    new Document("currentOp", 1)
                                            .append("$ownOps", true)
                                            .append("command.comment", queryId));

                    // If there are no results, the aggregation has not been executed yet or is complete.
                    // We throw an error.
                    // If there is a result, we can take the opId and kill the running operation.
                    final Object ops = currentOp.get("inprog");

                }
            }
            iterable = null;
            queryId = null;
        }
    }
}
