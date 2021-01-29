/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.documentdb.jdbc;

import software.amazon.documentdb.jdbc.common.ResultSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * DocumentDb implementation of ResultSet.
 */
public class DocumentDbResultSet extends ResultSet implements java.sql.ResultSet {
    private final List<String> columns;
    private final List<List<Object>> rows;
    private final boolean wasNull = false;
    private int rowIndex = -1;

    /**
     * DocumentDbResultSet constructor, initializes super class.
     * @param statement Statement Object.
     * @param columns The ordered name of the columns.
     * @param rows The result data.
     */
    DocumentDbResultSet(final java.sql.Statement statement, final List<String> columns, final List<List<Object>> rows) {
        super(statement);
        this.rows = rows;
        this.columns = columns;
    }

    @Override
    protected void doClose() { }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        // Do we want to update this or statement?
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {
        // Do we want to update this or statement?
    }

    @Override
    protected int getRowIndex() {
        return this.rowIndex;
    }

    @Override
    protected int getRowCount() {
        return this.rows.size();
    }

    @Override
    public boolean next() throws SQLException {
        // Increment row index, if it exceeds capacity, set it to 1 after the last element.
        if (++this.rowIndex >= rows.size()) {
            this.rowIndex = rows.size();
        }
        return (this.rowIndex < rows.size());
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        return columns.indexOf(columnLabel);
    }
}
