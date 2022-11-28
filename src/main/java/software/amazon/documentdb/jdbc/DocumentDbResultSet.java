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
import com.mongodb.client.MongoCursor;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.common.utilities.TypeConverters;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DocumentDb implementation of ResultSet.
 */
public class DocumentDbResultSet extends DocumentDbAbstractResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbResultSet.class);
    private static final int DEFAULT_FETCH_SIZE = 10; // 10 is default fetch size used by most JDBC drivers.
    private int fetchSize;
    private int rowIndex = -1;
    private final MongoCursor<Document> iterator;
    private Document current;
    private final List<String> paths;

    /**
     * DocumentDbResultSet constructor, initializes super class.
     */
    DocumentDbResultSet(
            final Statement statement,
            final MongoCursor<Document> iterator,
            final ImmutableList<JdbcColumnMetaData> columnMetaData,
            final List<String> paths) throws SQLException {
        super(statement, columnMetaData, true);
        this.iterator = iterator;

        // Set fetch size to be fetch size of statement if it exists. Otherwise, use default.
        this.fetchSize = statement != null ? statement.getFetchSize() : DEFAULT_FETCH_SIZE;
        this.paths = paths;
    }

    @Override
    protected void doClose() {
        iterator.close();
    }

    /**
     * Gets the current fetch size.
     * Getting and setting fetch size is accepted but will not be used for this particular driver.
     * @return the current fetch size.
     */
    @Override
    protected int getDriverFetchSize() {
        return this.fetchSize;
    }

    /**
     * Sets the current fetch size.
     * Getting and setting fetch size is accepted but will not be used for this particular driver.
     * @param rows The number of rows for the driver to fetch.
     */
    @Override
    protected void setDriverFetchSize(final int rows) {
        this.fetchSize = rows;
    }

    @Override
    protected int getRowIndex() {
        return rowIndex;
    }

    @Override
    protected int getRowCount() throws SQLFeatureNotSupportedException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.RESULT_FORWARD_ONLY);
    }

    @Override
    public boolean isLast() throws SQLException {
        verifyOpen();
        return (current != null && !iterator.hasNext());
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        verifyOpen();
        return (current == null && !iterator.hasNext());
    }

    @Override
    public boolean next() throws SQLException {
        verifyOpen();
        if (iterator.hasNext()) {
            current = iterator.next();
            rowIndex++;
            return true;
        } else {
            current = null;
            return false;
        }
    }

    @Override
    protected Object getValue(final int columnIndex) throws SQLException {
        final ResultSetMetaData metadata = getMetaData();
        final String path = paths.get(columnIndex - 1);

        if (path == null || path.isEmpty()) {
            throw SqlError.createSQLException(LOGGER, SqlState.DATA_EXCEPTION,
                    SqlError.CANNOT_RETRIEVE_COLUMN, metadata.getColumnName(columnIndex));
        }

        final String[] segmentedPath = path.split("\\.");
        Object segmentValue = current.get(segmentedPath[0]);
        for (int j = 1; j < segmentedPath.length && segmentValue instanceof Document; j++) {
            segmentValue = ((Document) segmentValue).get(segmentedPath[j]);
        }
        // Apache converters cannot handle the following types, must be specifically converted.
        if (segmentValue instanceof Binary) {
            return ((Binary) segmentValue).getData();
        }
        if (segmentValue instanceof Document) {
            return ((Document) segmentValue).toJson();
        }
        if (segmentValue instanceof List) {
            final List<?> modifiedList = ((List<?>) segmentValue)
                    .stream()
                    .map(o1 -> o1 instanceof Document ? ((Document) o1).toJson() : o1)
                    .collect(Collectors.toList());
            return modifiedList.toString();
        }
        if (segmentValue instanceof BsonTimestamp) {
            return TypeConverters.get(BsonTimestamp.class, Timestamp.class).convert(Timestamp.class, segmentValue);
        }

        return segmentValue;
    }
}
