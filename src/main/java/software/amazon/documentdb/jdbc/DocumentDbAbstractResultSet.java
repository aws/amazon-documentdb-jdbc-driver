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
import org.apache.commons.beanutils.ConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.common.utilities.TypeConverters;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Provides value processing.
 */
public abstract class DocumentDbAbstractResultSet extends
        software.amazon.documentdb.jdbc.common.ResultSet {
    private static final Logger LOGGER = LoggerFactory .getLogger(DocumentDbAbstractResultSet.class);
    private final ImmutableList<JdbcColumnMetaData> columnMetaData;
    private final Map<String, Integer> columnToIndexMap;
    private final int columnCount;
    private boolean wasNull = false;
    private ResultSetMetaData resultSetMetaData = null;
    private final boolean caseSensitive;

    /**
     * Instantiates the {@link DocumentDbAbstractResultSet} class. This will treat
     * column labels as case-insensitive.
     *
     * @param statement the statement that generated this result set.
     * @param columnMetaData the column metadata of the result set.
     */
    DocumentDbAbstractResultSet(
            final Statement statement,
            final ImmutableList<JdbcColumnMetaData> columnMetaData) {
        this(statement, columnMetaData, false);
    }

    /**
     * Instantiates the {@link DocumentDbAbstractResultSet} class.
     *
     * @param statement the statement that generated this result set.
     * @param columnMetaData the column metadata of the result set.
     * @param caseSensitive indicator of whether the column label should be case sensitive.
     */
    DocumentDbAbstractResultSet(
            final Statement statement,
            final ImmutableList<JdbcColumnMetaData> columnMetaData,
            final boolean caseSensitive) {
        super(statement);
        this.columnMetaData = columnMetaData;
        this.columnCount = columnMetaData.size();
        this.caseSensitive = caseSensitive;
        this.columnToIndexMap = buildColumnIndices(columnMetaData);
    }

    private Map<String, Integer> buildColumnIndices(
            final ImmutableList<JdbcColumnMetaData> columnMetaData) {
        final Map<String, Integer> columnIndices;
        columnIndices = caseSensitive
                ? new HashMap<>()
                : new TreeMap<>(String.CASE_INSENSITIVE_ORDER); // Note log(N) access
        for (JdbcColumnMetaData column : columnMetaData) {
            // Convert to one-indexed.
            columnIndices.put(column.getColumnLabel(), column.getOrdinal() + 1);
        }
        return columnIndices;
    }

    /**
     * Verifies that the current row is not before the first or after the last row.
     *
     * @throws SQLException if the current row is before the first or after the last row.
     */
    protected void verifyRow() throws SQLException {
        if (isBeforeFirst()) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.BEFORE_FIRST);
        } else if (isAfterLast()) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.AFTER_LAST);
        }
    }

    /**
     * Verifies that the given (one-based) column index is with in the expected range.
     *
     * @param columnIndex the column index to verify.
     * @throws SQLException if the column index is before the first or after the last column index.
     */
    protected void verifyColumnIndex(final int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INVALID_INDEX, columnIndex, columnCount);
        }
    }

    /**
     * Verifies that the the result set is open, the row is correct and the given column index is
     * valid.
     *
     * @param columnIndex the column index to verify.
     * @throws SQLException the result set is closed, the row is incorrect or the given
     * column index is invalid.
     */
    protected void verifyState(final int columnIndex) throws SQLException {
        verifyOpen();
        verifyRow();
        verifyColumnIndex(columnIndex);
    }

    /**
     * Gets the value in the target type on the current row and given index.
     *
     * @param columnIndex the index of the cell value.
     * @param targetType the intended target type.
     * @param <T> the intended target type.
     *
     * @return a value that is possibly converted to the target type.
     * @throws SQLException the result set is closed, the row is incorrect or the given
     *      * column index is invalid.
     */
    private <T> T getValue(final int columnIndex, final Class<T> targetType) throws SQLException {
        verifyState(columnIndex);
        final Object o = getValue(columnIndex);
        wasNull = (o == null);

        // If value is null, just use the target type as the source type.
        // This will ensure we get the default value.
        final Class<?> sourceType = wasNull ? targetType : o.getClass();

        try {
            return TypeConverters.get(sourceType, targetType).convert(targetType, o);
        } catch (ConversionException e) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    e,
                    SqlError.UNSUPPORTED_CONVERSION,
                    sourceType.getSimpleName(),
                    targetType.getSimpleName());
        }
    }

    /**
     * Gets the value of the cell are the current row and the given column index.
     *
     * @param columnIndex the (one-based) column index in the current row.
     *
     * @return the cell value.
     */
    protected abstract Object getValue(final int columnIndex) throws SQLException;

    @Override
    public boolean wasNull() throws SQLException {
        verifyOpen();
        return wasNull;
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        return getValue(columnIndex, String.class);
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        return getValue(columnIndex, boolean.class);
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        return getValue(columnIndex, byte.class);
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        return getValue(columnIndex, short.class);
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        return getValue(columnIndex, int.class);
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        return getValue(columnIndex, long.class);
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        return getValue(columnIndex, float.class);
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        return getValue(columnIndex, double.class);
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        return getValue(columnIndex, byte[].class);
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        return getDate(columnIndex, null);
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        return getTime(columnIndex, null);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, null);
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        final String value = getString(columnIndex);
        if (value == null) {
            return null;
        }
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        final byte[] value = getValue(columnIndex, byte[].class);
        if (value == null) {
            return null;
        }
        return new ByteArrayInputStream(value);
    }

    @Override
    public ResultSetMetaData getMetaData() {
        if (resultSetMetaData == null) {
            return new DocumentDbResultSetMetaData(columnMetaData);
        }
        return resultSetMetaData;
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        return getValue(columnIndex, Object.class);
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        final Integer columnIndex = columnToIndexMap.get(columnLabel);
        if (columnIndex == null) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.DATA_EXCEPTION,
                    SqlError.INVALID_COLUMN_LABEL,
                    columnLabel);
        }
        return columnIndex;
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        final String value = getValue(columnIndex, String.class);
        if (value == null) {
            return null;
        }
        return new StringReader(value);
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        return getValue(columnIndex, BigDecimal.class);
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        final Date value = getValue(columnIndex, Date.class);
        if (value == null) {
            return null;
        }
        return getMaybeAdjustedTime(value, cal);
    }

    private Date getMaybeAdjustedTime(final Date utcTime, final Calendar cal) {
        if (cal != null) {
            long adjustedTime = utcTime.getTime();
            adjustedTime -= cal.getTimeZone().getOffset(adjustedTime);
            return new Date(adjustedTime);
        }
        return utcTime;
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        final Date value = getDate(columnIndex, cal);
        if (value == null) {
            return null;
        }
        return new Time(value.getTime());
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        final Date value = getDate(columnIndex, cal);
        if (value == null) {
            return null;
        }
        return new Timestamp(value.getTime());
    }

    @Override
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        return getValue(columnIndex, type);
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        final byte[] bytes = getBytes(columnIndex);
        if (bytes == null) {
            return null;
        }
        return new SerialBlob(bytes);
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        final String value = getString(columnIndex);
        if (value == null) {
            return null;
        }
        return new SerialClob(value.toCharArray());
    }
}
