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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.query.DocumentDbMqlQueryContext;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * DocumentDb implementation of QueryExecution.
 */
public class DocumentDbQueryExecutor {
    private static final int OPERATION_CANCELLED_CODE = 11601;
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbQueryExecutor.class);
    private final Object queryStateLock = new Object();
    private final java.sql.Statement statement;
    private final DocumentDbConnectionProperties connectionProperties;
    private final DocumentDbQueryMappingService queryMapper;
    private int fetchSize;
    private int queryTimeout;
    private String queryId = null;
    private QueryState queryState = QueryState.NOT_STARTED;

    private enum QueryState {
        NOT_STARTED,
        IN_PROGRESS,
        CANCELED
    }

    /**
     * DocumentDbQueryExecutor constructor.
     */
    DocumentDbQueryExecutor(
            final java.sql.Statement statement,
            final DocumentDbConnectionProperties connectionProperties,
            final DocumentDbQueryMappingService queryMapper,
            final int queryTimeoutSecs,
            final int fetchSize) {
        this.statement = statement;
        this.connectionProperties = connectionProperties;
        this.queryMapper = queryMapper;
        this.fetchSize = fetchSize;
        this.queryTimeout = queryTimeoutSecs;
    }

    /**
     * This function wraps query cancellation and ensures query state is kept consistent.
     *
     * @throws SQLException If query cancellation fails or cannot be executed.
     * @param isClosing An indicator for whether the statement is closing.
     */
    protected void cancelQuery(final boolean isClosing) throws SQLException {
        synchronized (queryStateLock) {
            if (queryState.equals(QueryState.CANCELED)) {
                return;
            } else if (queryState.equals(QueryState.NOT_STARTED)) {
                if (isClosing) {
                    return;
                }
                throw SqlError.createSQLException(
                        LOGGER, SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_NOT_STARTED_OR_COMPLETE);
            }
            performCancel();
            queryState = QueryState.CANCELED;
        }
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
                        SqlError.QUERY_IN_PROGRESS);
            }
            queryState = QueryState.IN_PROGRESS;
            queryId = UUID.randomUUID().toString();
        }

        try {
            final java.sql.ResultSet resultSet = runQuery(query);
            synchronized (queryStateLock) {
                if (queryState.equals(QueryState.CANCELED)) {
                    resetQueryState();
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                }
            }
            return resultSet;
        } catch (final SQLException e) {
            throw e;
        } catch (final Exception e) {
            synchronized (queryStateLock) {
                if (e instanceof MongoException
                        && ((MongoException) e).getCode() == OPERATION_CANCELLED_CODE
                        && queryState.equals(QueryState.CANCELED)) {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.OPERATION_CANCELED,
                            SqlError.QUERY_CANCELED);
                } else {
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.CONNECTION_EXCEPTION,
                            SqlError.QUERY_FAILED, e);
                }
            }
        } finally {
            resetQueryState();
        }
    }

    /**
     * Function to execute query.
     * @param sql Query to execute.
     * @return java.sql.ResultSet object returned from query execution.
     */
    @VisibleForTesting
    protected java.sql.ResultSet runQuery(final String sql) throws SQLException {
        final Instant beginTranslation = Instant.now();

        LOGGER.info("Query {}: Beginning translation of query.", queryId);
        LOGGER.debug("Query {}: {}", queryId, sql);
        final long maxRows = statement.getLargeMaxRows();
        final DocumentDbMqlQueryContext queryContext = queryMapper.get(sql, maxRows);
        LOGGER.info("Query {}: Took {} ms to translate query.", queryId,
                Instant.now().toEpochMilli() - beginTranslation.toEpochMilli());
        if (!(statement.getConnection() instanceof DocumentDbConnection)) {
            throw new SQLException("Unexpected operation state.");
        }
        final Instant beginExecution = Instant.now();
        final DocumentDbConnection connection = (DocumentDbConnection) statement.getConnection();
        final DocumentDbConnectionProperties properties = connection.getConnectionProperties();
        final MongoClient client = connection.getMongoClient();

        final MongoDatabase database = client.getDatabase(properties.getDatabase());
        final MongoCollection<Document> collection = database
                .getCollection(queryContext.getCollectionName());

        // Only add limit if maxRows is non-zero.
        final List<Bson> aggregateOperations = queryContext.getAggregateOperations();

        AggregateIterable<Document> iterable = collection.aggregate(aggregateOperations);
        if (getQueryTimeout() > 0) {
            iterable = iterable.maxTime(getQueryTimeout(), TimeUnit.SECONDS);
        }
        if (getFetchSize() > 0) {
            iterable = iterable.batchSize(getFetchSize());
        }

        final ImmutableList<JdbcColumnMetaData> columnMetaData = ImmutableList
                .copyOf(queryContext.getColumnMetaData());
        final MongoCursor<Document> iterator = iterable.iterator();
        LOGGER.info("Query {}: Took {} ms to execute query and retrieve first batch of results.", queryId,
                Instant.now().toEpochMilli() - beginExecution.toEpochMilli());
        LOGGER.debug("Query {}: Executed on collection {} with following pipeline operations: {}",
                queryId, queryContext.getCollectionName(), queryContext.getAggregateOperations().toString());
        return new DocumentDbResultSet(
                this.statement,
                iterator,
                columnMetaData,
                queryContext.getPaths());
    }

    private void resetQueryState() {
        queryState = QueryState.NOT_STARTED;
        queryId = null;
    }

    private void performCancel() throws SQLException {
        final MongoClientSettings settings = connectionProperties.buildMongoClientSettings();
        try (MongoClient client = MongoClients.create(settings)) {
            final MongoDatabase database = client.getDatabase("admin");

            // Find the opId to kill using the queryId.
            final Document currentOp =
                    database.runCommand(
                            new Document("currentOp", 1)
                                    .append("$ownOps", true)
                                    .append("command.comment", queryId));

            if (!(currentOp.get("inprog") instanceof List)) {
                throw new SQLException("Unexpected operation state.");
            }
            final List<?> ops = (List<?>) currentOp.get("inprog");

            // If there are no results, the aggregation has not been executed yet or is complete.
            if (ops.isEmpty()) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_NOT_STARTED_OR_COMPLETE);
            }

            // If there is more than 1 result then more than 1 operations have been given same id,
            // and we do not know which to cancel.
            if (ops.size() != 1) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANNOT_BE_CANCELED,
                        "More than one running operation matched the query ID.");
            }

            if (!(ops.get(0) instanceof Document)) {
                throw new SQLException("Unexpected operation state.");
            }
            final Object opId = ((Document)ops.get(0)).get("opid");

            if (opId == null) {
                throw new SQLException("Unexpected operation state.");
            }

            // Cancel the aggregation using killOp.
            final Document killOp =
                    database.runCommand(new Document("killOp", 1)
                            .append("op", opId));

            // Throw error with info if command did not succeed.
            if (!killOp.get("ok").equals(1.0)) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.OPERATION_CANCELED,
                        SqlError.QUERY_CANNOT_BE_CANCELED,
                        killOp.get("info"));
            }

        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.OPERATION_CANCELED,
                    SqlError.QUERY_CANNOT_BE_CANCELED,
                    e);
        }
    }

    protected String getQueryId() {
        return queryId;
    }

    protected int getQueryTimeout() {
        return queryTimeout;
    }

    protected void setQueryTimeout(final int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    protected int getFetchSize() {
        return fetchSize;
    }

    protected void setFetchSize(final int fetchSize) {
        this.fetchSize = fetchSize;
    }
}
