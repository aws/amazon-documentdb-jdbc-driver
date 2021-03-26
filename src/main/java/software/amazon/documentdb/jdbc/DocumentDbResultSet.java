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
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.ResultSet;
import software.amazon.documentdb.jdbc.common.utilities.JdbcColumnMetaData;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;

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
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

/**
 * DocumentDb implementation of ResultSet.
 */
public class DocumentDbResultSet extends ResultSet implements java.sql.ResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbResultSet.class);
    private DocumentDbResultSetMetaData resultSetMetadata;
    private final ImmutableList<JdbcColumnMetaData> columnMetaData;
    private final Iterator<Document> iterator;

    /**
     * DocumentDbResultSet constructor, initializes super class.
     */
    DocumentDbResultSet(
            final Statement statement,
            final Iterator<Document> iterator,
            final ImmutableList<JdbcColumnMetaData> columnMetaData,
            final DocumentDbResultSetMetaData resultSetMetaData) throws SQLException {
        super(statement);
        this.iterator = iterator;
        this.columnMetaData = columnMetaData;
        this.resultSetMetadata = resultSetMetaData;
    }

    @Override
    protected void doClose() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    protected int getDriverFetchSize() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    protected void setDriverFetchSize(final int rows) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    protected int getRowIndex() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    protected int getRowCount() throws SQLFeatureNotSupportedException {
        throw SqlError.createSQLFeatureNotSupportedException(LOGGER, SqlError.RESULT_FORWARD_ONLY);
    }

    @Override
    public boolean next() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean wasNull() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSetMetadata;
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getType() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        //return resultSet.getDate(columnIndex, null);
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        //return resultSet.getTime(columnIndex, null);
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        //return resultSet.getTimestamp(columnIndex, null);
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        //return new SerialBlob(resultSet.getBytes(columnIndex)).getBinaryStream();
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        //return new SerialBlob(resultSet.getBytes(columnIndex));
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        //return new SerialBlob(resultSet.getBytes(columnLabel));
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        //return resultSet.getDate(columnLabel, null);
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        //return resultSet.getTime(columnLabel, null);
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        // Pass null for Calendar to ensure we use GMT
        //return resultSet.getTimestamp(columnLabel, null);
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        //return new SerialBlob(resultSet.getBytes(columnLabel)).getBinaryStream();
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal)
            throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getConcurrency() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCursorName() throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map)
            throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(final String columnLabel, final Map<String, Class<?>> map)
            throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(final String columnLabel) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        // TODO: Implement
        throw new SQLFeatureNotSupportedException();
    }
}
