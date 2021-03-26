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
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;


class DocumentDbListResultSet extends DocumentDbAbstractResultSet {
    private final List<List<Object>> metaData;
    private final int rowCount;
    private int rowIndex = -1;

    DocumentDbListResultSet(
            final Statement statement,
            final ImmutableList<JdbcColumnMetaData> columnMetaData,
            final List<List<Object>> metaData) {
        super(statement, columnMetaData);
        this.metaData = metaData;
        this.rowCount = metaData.size();
    }


    @Override
    protected Object getValue(final int columnIndex) {
        return metaData.get(getRowIndex()).get(columnIndex - 1);
    }

    @Override
    protected void doClose() {
        // no op
    }

    @Override
    protected int getDriverFetchSize() {
        return 0;
    }

    @Override
    protected void setDriverFetchSize(final int rows) {

    }

    @Override
    protected int getRowIndex() {
        // zero-indexed
        return rowIndex;
    }

    @Override
    protected int getRowCount() {
        return rowCount;
    }

    @Override
    public boolean next() throws SQLException {
        verifyOpen();
        if (getRowIndex() < getRowCount()) {
            rowIndex++;
        }
        return getRowIndex() < getRowCount();
    }

    @Override
    public boolean isBeforeFirst() {
        return getRowIndex() < 0;
    }

    @Override
    public boolean isAfterLast() {
        return getRowIndex() >= getRowCount();
    }

    @Override
    public boolean isFirst() {
        return rowIndex == 0;
    }

    @Override
    public boolean isLast() {
        return getRowIndex() == getRowCount() - 1;
    }

    @Override
    public void beforeFirst() {
        rowIndex = -1;
    }

    @Override
    public void afterLast() {
        rowIndex = getRowCount();
    }

    @Override
    public boolean first() {
        rowIndex = 0;
        return getRowIndex() < getRowCount();
    }

    @Override
    public boolean last() {
        rowIndex = getRowCount() - 1;
        return getRowIndex() >= 0;
    }

    @Override
    public boolean absolute(final int row) {
        if (row > 0 && row < getRowCount()) {
            rowIndex = row - 1;
            return true;
        } else if (row < 0) {
            if (getRowCount() + row >= 0) {
                rowIndex = getRowCount() + row;
                return true;
            }
            return false;
        } else {
            rowIndex = -1;
            return false;
        }
    }

    @Override
    public boolean relative(final int rows) {
        final int proposedRowIndex = getRowIndex() + rows;
        if (proposedRowIndex < 0) {
            rowIndex = -1;
            return false;
        } else if (proposedRowIndex >= getRowCount()) {
            rowIndex = getRowCount();
            return  false;
        }
        rowIndex = proposedRowIndex;
        return true;
    }

    @Override
    public boolean previous() {
        if (getRowIndex() >= 0) {
            rowIndex--;
        }
        return getRowIndex() >= 0;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }
}
