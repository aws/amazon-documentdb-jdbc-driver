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
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.query.DocumentDbMqlQueryContext;
import software.amazon.documentdb.jdbc.query.DocumentDbQueryMappingService;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.TimeUnit;

/**
 * DocumentDb implementation of QueryExecution.
 */
public class DocumentDbQueryExecutor {
    private final int fetchSize;
    private final java.sql.Statement statement;
    private final String uri;
    private final int queryTimeout;
    private final DocumentDbQueryMappingService queryMapper;

    /**
     * DocumentDbQueryExecutor constructor.
     */
    DocumentDbQueryExecutor(
            final java.sql.Statement statement,
            final String uri,
            final DocumentDbQueryMappingService queryMapper,
            final int queryTimeoutSecs,
            final int fetchSize) {
        this.statement = statement;
        this.uri = uri;
        this.queryMapper = queryMapper;
        this.fetchSize = fetchSize;
        this.queryTimeout = queryTimeoutSecs;
    }

    protected void cancelQuery() throws SQLException {
        // TODO: Cancel logic.
        throw new SQLFeatureNotSupportedException();
    }

    protected int getFetchSize() throws SQLException {
        return fetchSize;
    }

    /**
     * Function to execute query.
     * @param sql Query to execute.
     * @return java.sql.ResultSet object returned from query execution.
     */
    public java.sql.ResultSet executeQuery(final String sql) throws SQLException {
        final DocumentDbMqlQueryContext queryContext = queryMapper.get(sql);

        if (!(statement.getConnection() instanceof DocumentDbConnection)) {
            throw new SQLException("Unexpected operation state.");
        }

        final DocumentDbConnection connection = (DocumentDbConnection) statement.getConnection();
        final DocumentDbConnectionProperties properties = connection.getConnectionProperties();
        final MongoClientSettings settings = properties.buildMongoClientSettings();
        final MongoClient client = MongoClients.create(settings);
        final MongoDatabase database = client.getDatabase(properties.getDatabase());
        final MongoCollection<Document> collection = database
                .getCollection(queryContext.getCollectionName());
        AggregateIterable<Document> iterable = collection
                .aggregate(queryContext.getAggregateOperations());
        if (getQueryTimeout() > 0) {
            iterable = iterable.maxTime(getQueryTimeout(), TimeUnit.SECONDS);
        }
        if (getFetchSize() > 0) {
            iterable = iterable.batchSize(getFetchSize());
        }
        final MongoCursor<Document> iterator = iterable.iterator();

        final ImmutableList<JdbcColumnMetaData> columnMetaData = ImmutableList
                .copyOf(queryContext.getColumnMetaData());
        return new DocumentDbResultSet(
                this.statement,
                iterator,
                columnMetaData,
                queryContext.getPaths(),
                client);
    }

    /**
     * Get query execution timeout in seconds.
     * @return Query execution timeout in seconds.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }
}
