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

import software.amazon.documentdb.jdbc.common.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DocumentDb implementation of DatabaseMetaData.
 */
public class DocumentDbDatabaseMetadata extends DatabaseMetaData implements java.sql.DatabaseMetaData {

    private final java.sql.DatabaseMetaData databaseMetaData;
    /**
     * DocumentDbDatabaseMetadata constructor, initializes super class.
     * @param databaseMetaData the embedded DatabaseMetaData.
     */
    DocumentDbDatabaseMetadata(final java.sql.DatabaseMetaData databaseMetaData)
            throws SQLException {
        super(databaseMetaData.getConnection());
        this.databaseMetaData = databaseMetaData;
    }

    // TODO: Go through and implement these functions
    @Override
    public String getURL() throws SQLException {
        return databaseMetaData.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return databaseMetaData.getUserName();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return databaseMetaData.getDatabaseProductName();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return databaseMetaData.getDatabaseProductVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return databaseMetaData.getDriverName();
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return databaseMetaData.getSQLKeywords();
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return databaseMetaData.getNumericFunctions();
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return databaseMetaData.getStringFunctions();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return databaseMetaData.getSystemFunctions();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return databaseMetaData.getTimeDateFunctions();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return databaseMetaData.getSearchStringEscape();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return databaseMetaData.getExtraNameCharacters();
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return databaseMetaData.getCatalogTerm();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return databaseMetaData.getCatalogSeparator();
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return databaseMetaData.getMaxRowSize();
    }

    @Override
    public ResultSet getProcedures(final String catalog, final String schemaPattern,
            final String procedureNamePattern) throws SQLException {
        return databaseMetaData.getProcedures(catalog, schemaPattern, procedureNamePattern);
    }

    @Override
    public ResultSet getTables(final String catalog, final String schemaPattern,
            final String tableNamePattern, final String[] types) throws SQLException {
        return databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return databaseMetaData.getSchemas();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return databaseMetaData.getCatalogs();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return databaseMetaData.getTableTypes();
    }

    @Override
    public ResultSet getColumns(final String catalog, final String schemaPattern,
            final String tableNamePattern, final String columnNamePattern) throws SQLException {
        return databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern,
                columnNamePattern);
    }

    @Override
    public ResultSet getColumnPrivileges(final String catalog, final String schema,
            final String table, final String columnNamePattern)
            throws SQLException {
        return databaseMetaData.getColumnPrivileges(catalog, schema, table, columnNamePattern);
    }

    @Override
    public ResultSet getBestRowIdentifier(final String catalog, final String schema,
            final String table, final int scope, final boolean nullable)
            throws SQLException {
        return databaseMetaData.getBestRowIdentifier(catalog, schema, table, scope, nullable);
    }

    @Override
    public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table)
            throws SQLException {
        return databaseMetaData.getPrimaryKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
            throws SQLException {
        return databaseMetaData.getImportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return databaseMetaData.getTypeInfo();
    }

    @Override
    public ResultSet getIndexInfo(final String catalog, final String schema, final String table,
            final boolean unique, final boolean approximate) throws SQLException {
        return databaseMetaData.getIndexInfo(catalog, schema, table, unique, approximate);
    }

    @Override
    public ResultSet getAttributes(final String catalog, final String schemaPattern,
            final String typeNamePattern, final String attributeNamePattern) throws SQLException {
        return databaseMetaData.getAttributes(catalog, schemaPattern, typeNamePattern,
                attributeNamePattern);
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return databaseMetaData.getDatabaseMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return databaseMetaData.getDatabaseMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return databaseMetaData.getJDBCMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return databaseMetaData.getJDBCMinorVersion();
    }

    @Override
    public ResultSet getSchemas(final String catalog, final String schemaPattern)
            throws SQLException {
        return databaseMetaData.getSchemas(catalog, schemaPattern);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return databaseMetaData.getClientInfoProperties();
    }
}
