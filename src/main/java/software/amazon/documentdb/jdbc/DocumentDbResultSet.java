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
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

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

    /**
     * DocumentDbResultSet constructor, initializes super class.
     */
    DocumentDbResultSet(
            final Statement statement,
            final MongoCursor<Document> iterator,
            final ImmutableList<JdbcColumnMetaData> columnMetaData) throws SQLException {
        super(statement, columnMetaData, true);
        this.iterator = iterator;

        // Set fetch size to be fetch size of statement if it exists. Otherwise, use default.
        this.fetchSize = statement != null ? statement.getFetchSize() : DEFAULT_FETCH_SIZE;
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
        // TODO: Implement in [AD-8]
        return null;
    }
}
