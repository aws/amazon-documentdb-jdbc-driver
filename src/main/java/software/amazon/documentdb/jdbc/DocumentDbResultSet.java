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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.ResultSet;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import javax.sql.rowset.serial.SerialBlob;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * DocumentDb implementation of ResultSet.
 */
public class DocumentDbResultSet extends ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbResultSet.class);
    private final java.sql.ResultSet resultSet;

    /**
     * DocumentDbResultSet constructor, initializes super class.
     * @param resultSet the underlying result set.
     */
    DocumentDbResultSet(final java.sql.ResultSet resultSet) throws SQLException {
        super(resultSet.getStatement());
        this.resultSet = resultSet;
    }

    @Override
    protected void doClose() throws SQLException {
        resultSet.close();
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        return resultSet.getFetchSize();
    }

    @Override
    protected void setDriverFetchSize(final int rows) throws SQLException {
        resultSet.setFetchSize(rows);
    }

    @Override
    protected int getRowIndex() throws SQLFeatureNotSupportedException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.RESULT_FORWARD_ONLY);
    }

    @Override
    protected int getRowCount() throws SQLFeatureNotSupportedException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.RESULT_FORWARD_ONLY);
    }

    @Override
    public boolean next() throws SQLException {
        return resultSet.next();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return resultSet.wasNull();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new DocumentDbResultSetMetadata(resultSet.getMetaData());
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        return resultSet.findColumn(columnLabel);
    }

    @Override
    public int getType() throws SQLException {
        return resultSet.getType();
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        return resultSet.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        return resultSet.getBoolean(columnIndex);
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        return resultSet.getByte(columnIndex);
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        return resultSet.getShort(columnIndex);
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        return resultSet.getInt(columnIndex);
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        return resultSet.getLong(columnIndex);
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        return resultSet.getFloat(columnIndex);
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        return resultSet.getDouble(columnIndex);
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        return resultSet.getBytes(columnIndex);
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        return resultSet.getDate(columnIndex, null);
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        return resultSet.getTime(columnIndex, null);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        return resultSet.getTimestamp(columnIndex, null);
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        return resultSet.getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        return new SerialBlob(resultSet.getBytes(columnIndex)).getBinaryStream();
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        return new SerialBlob(resultSet.getBytes(columnIndex));
    }

    @Override
    public Array getArray(final int columnIndex) throws SQLException {
        return resultSet.getArray(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        return resultSet.getTimestamp(columnIndex, cal);
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
        return resultSet.getNString(columnIndex);
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        return resultSet.getDate(columnIndex, cal);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        return resultSet.getTime(columnIndex, cal);
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        return new SerialBlob(resultSet.getBytes(columnLabel));
    }

    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
        return resultSet.getClob(columnLabel);
    }

    @Override
    public Array getArray(final String columnLabel) throws SQLException {
        return resultSet.getArray(columnLabel);
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
        return resultSet.getString(columnLabel);
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        return resultSet.getBoolean(columnLabel);
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        return resultSet.getByte(columnLabel);
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
        return resultSet.getShort(columnLabel);
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
        return resultSet.getInt(columnLabel);
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
        return resultSet.getLong(columnLabel);
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        return resultSet.getFloat(columnLabel);
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        return resultSet.getDouble(columnLabel);
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        return resultSet.getBytes(columnLabel);
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        return resultSet.getDate(columnLabel, null);
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        return resultSet.getTime(columnLabel, null);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        return resultSet.getTimestamp(columnLabel, null);
    }

    @Override
    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
        return resultSet.getAsciiStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        return new SerialBlob(resultSet.getBytes(columnLabel)).getBinaryStream();
    }

    @Override
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        return resultSet.getCharacterStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        return resultSet.getBigDecimal(columnLabel);
    }

    @Override
    public String getNString(final String columnLabel) throws SQLException {
        return resultSet.getNString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(final String columnLabel) throws SQLException {
        return resultSet.getNCharacterStream(columnLabel);
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
        return resultSet.getDate(columnLabel, cal);
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
        return resultSet.getTime(columnLabel, cal);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal)
            throws SQLException {
        return resultSet.getTimestamp(columnLabel, cal);
    }

    @Override
    public int getHoldability() throws SQLException {
        return resultSet.getHoldability();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return resultSet.getConcurrency();
    }

    @Override
    public String getCursorName() throws SQLException {
        return resultSet.getCursorName();
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        return resultSet.getBigDecimal(columnIndex);
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map)
            throws SQLException {
        return resultSet.getObject(columnIndex, map);
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        return resultSet.getObject(columnIndex);
    }

    @Override
    public Ref getRef(final int columnIndex) throws SQLException {
        return resultSet.getRef(columnIndex);
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        return resultSet.getClob(columnIndex);
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        return resultSet.getURL(columnIndex);
    }

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {
        return resultSet.getRowId(columnIndex);
    }

    @Override
    public NClob getNClob(final int columnIndex) throws SQLException {
        return resultSet.getNClob(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
        return resultSet.getSQLXML(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {
        return resultSet.getNCharacterStream(columnIndex);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        return resultSet.getObject(columnIndex, type);
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        return resultSet.getCharacterStream(columnIndex);
    }

    @Override
    public Object getObject(final String columnLabel, final Map<String, Class<?>> map)
            throws SQLException {
        return resultSet.getObject(columnLabel, map);
    }

    @Override
    public Ref getRef(final String columnLabel) throws SQLException {
        return resultSet.getRef(columnLabel);
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        return resultSet.getObject(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
        return resultSet.getSQLXML(columnLabel);
    }

    @Override
    public URL getURL(final String columnLabel) throws SQLException {
        return resultSet.getURL(columnLabel);
    }

    @Override
    public RowId getRowId(final String columnLabel) throws SQLException {
        return resultSet.getRowId(columnLabel);
    }

    @Override
    public NClob getNClob(final String columnLabel) throws SQLException {
        return resultSet.getNClob(columnLabel);
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        return resultSet.getObject(columnLabel, type);
    }
}
