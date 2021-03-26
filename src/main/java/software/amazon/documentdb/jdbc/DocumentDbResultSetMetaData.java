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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.ResultSetMetaData;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import java.sql.SQLException;

/**
 * DocumentDb implementation of ResultSetMetadata.
 */
public class DocumentDbResultSetMetaData extends ResultSetMetaData implements java.sql.ResultSetMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbResultSetMetaData.class);
    private final ImmutableList<JdbcColumnMetaData> columnMetaData;
    private final int columnCount;

    DocumentDbResultSetMetaData(final ImmutableList<JdbcColumnMetaData> columnMetaData) {
        this.columnMetaData = columnMetaData;
        this.columnCount = columnMetaData.size();
    }

    private void verifyColumnIndex(final int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INVALID_INDEX, columnIndex, columnCount);
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public boolean isAutoIncrement(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).isCaseSensitive();
    }

    @Override
    public boolean isSearchable(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).isSearchable();
    }

    @Override
    public boolean isCurrency(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).isCurrency();
    }

    @Override
    public int isNullable(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getNullable();
    }

    @Override
    public boolean isSigned(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).isSigned();
    }

    @Override
    public int getColumnDisplaySize(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getColumnDisplaySize();
    }

    @Override
    public String getColumnLabel(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getColumnLabel();
    }

    @Override
    public String getColumnName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getColumnName();
    }

    @Override
    public String getSchemaName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getSchemaName();
    }

    @Override
    public int getPrecision(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getPrecision();
    }

    @Override
    public int getScale(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getScale();
    }

    @Override
    public String getTableName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getTableName();
    }

    @Override
    public String getCatalogName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getCatalogName();
    }

    @Override
    public int getColumnType(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getColumnType();
    }

    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getColumnTypeName();
    }

    @Override
    public boolean isReadOnly(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).isReadOnly();
    }

    @Override
    public boolean isWritable(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).isWritable();
    }

    @Override
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).isDefinitelyWritable();
    }

    @Override
    public String getColumnClassName(final int column) throws SQLException {
        verifyColumnIndex(column);
        return columnMetaData.get(column - 1).getColumnClassName();
    }
}
