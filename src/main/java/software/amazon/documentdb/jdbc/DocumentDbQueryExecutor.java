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

import org.apache.calcite.avatica.ColumnMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;

/**
 * DocumentDb implementation of QueryExecution.
 */
public class DocumentDbQueryExecutor {
    private static final int MAX_FETCH_SIZE = Integer.MAX_VALUE;
    private final java.sql.Statement statement;
    private final String uri;
    private int queryTimeout = -1;
    private final DocumentDbQueryMapper queryMapper;

    /**
     * DocumentDbQueryExecutor constructor.
     * @param statement java.sql.Statement Object.
     * @param uri Endpoint to execute queries against.
     */
    DocumentDbQueryExecutor(final java.sql.Statement statement, final String uri, final DocumentDbQueryMapper queryMapper) {
        this.uri = uri;
        this.statement = statement;
        this.queryMapper = queryMapper;
        // TODO: Add way of getting and setting connection properties here.
    }

    protected void cancelQuery() throws SQLException {
        // TODO: Cancel logic.
    }

    protected int getMaxFetchSize() throws SQLException {
        return MAX_FETCH_SIZE;
    }

    /**
     * Function to execute query.
     * @param sql Query to execute.
     * @return java.sql.ResultSet object returned from query execution.
     */
    public java.sql.ResultSet executeQuery(final String sql) {
        final DocumentDbMqlQueryContext mql = queryMapper.getMqlQueryContext(sql);
        // TODO: Do the query here somehow.

        // Construct the result set
        final ResultSetMetaData metaData = new POCResultSetMetaData(mql.getColumnMetaData());
        return new POCResultSet(this.statement, metaData);
    }

    /**
     * Get query execution timeout in seconds.
     * @return Query execution timeout in seconds.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Set query execution timeout to the timeout in seconds.
     * @param seconds Time in seconds to set query timeout to.
     */
    public void setQueryTimeout(final int seconds) {
        queryTimeout = seconds;
    }

    // TODO: Implement ResultSet and remove this.
    /** POC of a result set that doesn't wrap the Avatica ResultSet. Not much yet. **/
    private static class POCResultSet extends software.amazon.documentdb.jdbc.common.ResultSet {
        private ResultSetMetaData metaData;
        protected POCResultSet(final Statement statement, final ResultSetMetaData metaData) {
            super(statement);
            this.metaData = metaData;
        }

        @Override
        protected void doClose() throws SQLException {

        }

        @Override
        protected int getDriverFetchSize() throws SQLException {
            return 0;
        }

        @Override
        protected void setDriverFetchSize(final int rows) throws SQLException {

        }

        @Override
        protected int getRowIndex() throws SQLFeatureNotSupportedException {
            return 0;
        }

        @Override
        protected int getRowCount() throws SQLFeatureNotSupportedException {
            return 0;
        }

        @Override
        public boolean next() throws SQLException {
            return false;
        }

        @Override
        public boolean wasNull() throws SQLException {
            return false;
        }

        @Override
        public ResultSetMetaData getMetaData() throws SQLException {
            return this.metaData;
        }

        @Override
        public int findColumn(final String columnLabel) throws SQLException {
            return 0;
        }
    }

    // TODO: Implement ResultSetMetaData and remove this.
    /**
     * POC of ResultSetMetadata that doesn't wrap AvaticaResultMetadata. Relies entirely on the
     * column metadata from Calcite.
     */
    private static class POCResultSetMetaData extends software.amazon.documentdb.jdbc.common.ResultSetMetaData {
        private final List<ColumnMetaData> columnMetaDataList;

        POCResultSetMetaData(final List<ColumnMetaData> columnMetaDataList) {
            this.columnMetaDataList = columnMetaDataList;
        }
        @Override
        public int getColumnCount() {
            return columnMetaDataList.size();
        }

        @Override
        public boolean isAutoIncrement(final int column) {
            return columnMetaDataList.get(column - 1).autoIncrement;
        }

        @Override
        public boolean isCaseSensitive(final int column) {
            return columnMetaDataList.get(column - 1).caseSensitive;
        }

        @Override
        public boolean isSearchable(final int column) {
            return columnMetaDataList.get(column - 1).searchable;
        }

        @Override
        public boolean isCurrency(final int column) {
            return columnMetaDataList.get(column - 1).currency;
        }

        @Override
        public int isNullable(final int column) {
            return columnMetaDataList.get(column - 1).nullable;
        }

        @Override
        public boolean isSigned(final int column) {
            return columnMetaDataList.get(column - 1).signed;
        }

        @Override
        public int getColumnDisplaySize(final int column) {
            return columnMetaDataList.get(column - 1).displaySize;
        }

        @Override
        public String getColumnLabel(final int column) {
            return columnMetaDataList.get(column - 1).label;
        }

        @Override
        public String getColumnName(final int column) {
            return columnMetaDataList.get(column - 1).columnName;
        }

        @Override
        public String getSchemaName(final int column) {
            return columnMetaDataList.get(column - 1).schemaName;
        }

        @Override
        public int getPrecision(final int column) {
            return columnMetaDataList.get(column - 1).precision;
        }

        @Override
        public int getScale(final int column) {
            return columnMetaDataList.get(column - 1).scale;
        }

        @Override
        public String getTableName(final int column) {
            return columnMetaDataList.get(column - 1).tableName;
        }

        @Override
        public String getCatalogName(final int column) {
            return columnMetaDataList.get(column - 1).catalogName;
        }

        @Override
        public int getColumnType(final int column) {
            return columnMetaDataList.get(column - 1).type.id;
        }

        @Override
        public String getColumnTypeName(final int column) {
            return columnMetaDataList.get(column - 1).type.name;
        }

        @Override
        public boolean isReadOnly(final int column) {
            return columnMetaDataList.get(column - 1).readOnly;
        }

        @Override
        public boolean isWritable(final int column) {
            return columnMetaDataList.get(column - 1).writable;
        }

        @Override
        public boolean isDefinitelyWritable(final int column) {
            return columnMetaDataList.get(column - 1).writable;
        }

        @Override
        public String getColumnClassName(final int column) {
            return columnMetaDataList.get(column - 1).columnClassName;
        }
    }
}
