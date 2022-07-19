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
import software.amazon.documentdb.jdbc.common.utilities.JdbcType;
import java.sql.ResultSetMetaData;
import java.sql.Types;

/**
 * Contains static methods to get DatabaseMetadata column metadata instances.
 */
class DocumentDbDatabaseMetaDataResultSets {
    private static ImmutableList<JdbcColumnMetaData> proceduresColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> tablesColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> schemasColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> catalogsColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> tableTypesColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> columnsColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> columnPrivilegesColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> attributesColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> primaryKeysColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> importedKeysColumnMetaData;
    private static ImmutableList<JdbcColumnMetaData> typeInfoColumnMetaData;

    static ImmutableList<JdbcColumnMetaData> buildProceduresColumnMetaData(
            final String schemaName) {
        if (proceduresColumnMetaData == null) {
            // 1. PROCEDURE_CAT String => procedure catalog (may be null)
            // 2. PROCEDURE_SCHEM String => procedure schema (may be null)
            // 3. PROCEDURE_NAME String => procedure name
            // 4. reserved for future use
            // 5. reserved for future use
            // 6. reserved for future use
            // 7. REMARKS String => explanatory comment on the procedure
            // 8. PROCEDURE_TYPE short => kind of procedure:
            //        procedureResultUnknown - Cannot determine if a return value will be returned
            //        procedureNoResult - Does not return a return value
            //        procedureReturnsResult - Returns a return value
            // 9. SPECIFIC_NAME String => The name which uniquely identifies this procedure within its schema.
            int ordinal = 0;
            proceduresColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PROCEDURE_CAT", //label,
                            "PROCEDURE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PROCEDURE_SCHEM", //label,
                            "PROCEDURE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PROCEDURE_NAME", //label,
                            "PROCEDURE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "FUTURE_USE1", //label,
                            "FUTURE_USE1", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "FUTURE_USE2", //label,
                            "FUTURE_USE2", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "FUTURE_USE3", //label,
                            "FUTURE_USE3", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "REMARKS", //label,
                            "REMARKS", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PROCEDURE_TYPE", //label,
                            "PROCEDURE_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "SPECIFIC_NAME", //label,
                            "SPECIFIC_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .build();
        }
        return proceduresColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildTablesColumnMetaData(
            final String schemaName) {
        if (tablesColumnMetaData == null) {
            // 1. TABLE_CAT String => table catalog (may be null)
            // 2. TABLE_SCHEM String => table schema (may be null)
            // 3. TABLE_NAME String => table name
            // 4. TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
            // 5. REMARKS String => explanatory comment on the table
            // 6. TYPE_CAT String => the types catalog (may be null)
            // 7. TYPE_SCHEM String => the types schema (may be null)
            // 8. TYPE_NAME String => type name (may be null)
            // 9. SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
            // 10. REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
            int ordinal = 0;
            tablesColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_CAT", //label,
                            "TABLE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_SCHEM", //label,
                            "TABLE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_NAME", //label,
                            "TABLE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_TYPE", //label,
                            "TABLE_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "REMARKS", //label,
                            "REMARKS", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TYPE_CAT", //label,
                            "TYPE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TYPE_SCHEM", //label,
                            "TYPE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TYPE_NAME", //label,
                            "TYPE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "SELF_REFERENCING_COL_NAME", //label,
                            "SELF_REFERENCING_COL_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "REF_GENERATION", //label,
                            "REF_GENERATION", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .build();
        }
        return tablesColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildSchemasColumnMetaData(
            final String schemaName) {
        if (schemasColumnMetaData == null) {
            int ordinal = 0;
            schemasColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_SCHEM", //label,
                            "TABLE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_CATALOG", //label,
                            "TABLE_CATALOG", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .build();
        }
        return schemasColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildCatalogsColumnMetaData(
            final String schemaName) {
        if (catalogsColumnMetaData == null) {
            // 1. TABLE_CAT String => catalog name
            final int ordinal = 0;
            catalogsColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_CAT", //label,
                            "TABLE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .build();
        }
        return catalogsColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildTableTypesColumnMetaData(
            final String schemaName) {
        if (tableTypesColumnMetaData == null) {
            // 1. TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
            final int ordinal = 0;
            tableTypesColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_TYPE", //label,
                            "TABLE_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .build();
        }
        return tableTypesColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildColumnsColumnMetaData(
            final String schemaName) {
        if (columnsColumnMetaData == null) {
            //  1. TABLE_CAT String => table catalog (may be null)
            //  2. TABLE_SCHEM String => table schema (may be null)
            //  3. TABLE_NAME String => table name
            //  4. COLUMN_NAME String => column name
            //  5. DATA_TYPE int => SQL type from java.sql.Types
            //  6. TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
            //  7. COLUMN_SIZE int => column size.
            //  8. BUFFER_LENGTH is not used.
            //  9. DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
            // 10. NUM_PREC_RADIX int => Radix (typically either 10 or 2)
            // 11. NULLABLE int => is NULL allowed.
            //        columnNoNulls - might not allow NULL values
            //        columnNullable - definitely allows NULL values
            //        columnNullableUnknown - nullability unknown
            // 12. REMARKS String => comment describing column (may be null)
            // 13. COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
            // 14. SQL_DATA_TYPE int => unused
            // 15. SQL_DATETIME_SUB int => unused
            // 16. CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
            // 17. ORDINAL_POSITION int => index of column in table (starting at 1)
            // 18. IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
            //        YES --- if the column can include NULLs
            //        NO --- if the column cannot include NULLs
            //        empty string --- if the nullability for the column is unknown
            // 19. SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
            // 20. SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
            // 21. SCOPE_TABLE String => table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
            // 22. SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
            // 23. IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
            //        YES --- if the column is auto incremented
            //        NO --- if the column is not auto incremented
            //        empty string --- if it cannot be determined whether the column is auto incremented
            // 24. IS_GENERATEDCOLUMN String => Indicates whether this is a generated column
            //        YES --- if this a generated column
            //        NO --- if this not a generated column
            //        empty string --- if it cannot be determined whether this is a generated column
            int ordinal = 0;
            columnsColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_CAT", //label,
                            "TABLE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_SCHEM", //label,
                            "TABLE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            120, //displaySize,
                            "TABLE_NAME", //label,
                            "TABLE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "COLUMN_NAME", //label,
                            "COLUMN_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "DATA_TYPE", //label,
                            "DATA_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TYPE_NAME", //label,
                            "TYPE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "COLUMN_SIZE", //label,
                            "COLUMN_SIZE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "BUFFER_LENGTH", //label,
                            "BUFFER_LENGTH", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "DECIMAL_DIGITS", //label,
                            "DECIMAL_DIGITS", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "NUM_PREC_RADIX", //label,
                            "NUM_PREC_RADIX", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            2, //displaySize,
                            "NULLABLE", //label,
                            "NULLABLE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "REMARKS", //label,
                            "REMARKS", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "COLUMN_DEF", //label,
                            "COLUMN_DEF", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "SQL_DATA_TYPE", //label,
                            "SQL_DATA_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "SQL_DATETIME_SUB", //label,
                            "SQL_DATETIME_SUB", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "CHAR_OCTET_LENGTH", //label,
                            "CHAR_OCTET_LENGTH", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            12, //displaySize,
                            "ORDINAL_POSITION", //label,
                            "ORDINAL_POSITION", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            12, //displaySize,
                            "IS_NULLABLE", //label,
                            "IS_NULLABLE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "SCOPE_CATALOG", //label,
                            "SCOPE_CATALOG", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "SCOPE_SCHEMA", //label,
                            "SCOPE_SCHEMA", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            120, //displaySize,
                            "SCOPE_TABLE", //label,
                            "SCOPE_TABLE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "SOURCE_DATA_TYPE", //label,
                            "SOURCE_DATA_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            3, //displaySize,
                            "IS_AUTOINCREMENT", //label,
                            "IS_AUTOINCREMENT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            3, //displaySize,
                            "IS_GENERATEDCOLUMN", //label,
                            "IS_GENERATEDCOLUMN", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .build();
        }
        return columnsColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildColumnPrivilegesColumnMetaData(
            final String schemaName) {
        if (columnPrivilegesColumnMetaData == null) {
            // 1. TABLE_CAT String => table catalog (may be null)
            // 2. TABLE_SCHEM String => table schema (may be null)
            // 3. TABLE_NAME String => table name
            // 4. COLUMN_NAME String => column name
            // 5. GRANTOR String => grantor of access (may be null)
            // 6. GRANTEE String => grantee of access
            // 7. PRIVILEGE String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)
            // 8. IS_GRANTABLE String => "YES" if grantee is permitted to grant to others; "NO" if not; null if unknown
            int ordinal = 0;
            columnPrivilegesColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_CAT", //label,
                            "TABLE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_SCHEM", //label,
                            "TABLE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_NAME", //label,
                            "TABLE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "COLUMN_NAME", //label,
                            "COLUMN_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "GRANTOR", //label,
                            "GRANTOR", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "GRANTEE", //label,
                            "GRANTEE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PRIVILEGE", //label,
                            "PRIVILEGE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "IS_GRANTABLE", //label,
                            "IS_GRANTABLE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .build();
        }
        return columnPrivilegesColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildAttributesColumnMetaData(
            final String schemaName) {
        if (attributesColumnMetaData == null) {
            //  1. TYPE_CAT String => type catalog (may be null)
            //  2. TYPE_SCHEM String => type schema (may be null)
            //  3. TYPE_NAME String => type name
            //  4. ATTR_NAME String => attribute name
            //  5. DATA_TYPE int => attribute type SQL type from java.sql.Types
            //  6. ATTR_TYPE_NAME String => Data source dependent type name. For a UDT, the type name is fully qualified. For a REF, the type name is fully qualified and represents the target type of the reference type.
            //  7. ATTR_SIZE int => column size. For char or date types this is the maximum number of characters; for numeric or decimal types this is precision.
            //  8. DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
            //  9. NUM_PREC_RADIX int => Radix (typically either 10 or 2)
            // 10. NULLABLE int => whether NULL is allowed
            //        attributeNoNulls - might not allow NULL values
            //        attributeNullable - definitely allows NULL values
            //        attributeNullableUnknown - nullability unknown
            // 11. REMARKS String => comment describing column (may be null)
            // 12. ATTR_DEF String => default value (may be null)
            // 13. SQL_DATA_TYPE int => unused
            // 14. SQL_DATETIME_SUB int => unused
            // 15. CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
            // 16. ORDINAL_POSITION int => index of the attribute in the UDT (starting at 1)
            // 17. IS_NULLABLE String => ISO rules are used to determine the nullability for a attribute.
            //        YES --- if the attribute can include NULLs
            //        NO --- if the attribute cannot include NULLs
            //        empty string --- if the nullability for the attribute is unknown
            // 18. SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
            // 19. SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
            // 20. SCOPE_TABLE String => table name that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
            // 21. SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type,SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
            int ordinal = 0;
            attributesColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TYPE_CAT", //label,
                            "TYPE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TYPE_SCHEM", //label,
                            "TYPE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            120, //displaySize,
                            "TYPE_NAME", //label,
                            "TYPE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "ATTR_NAME", //label,
                            "ATTR_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "DATA_TYPE", //label,
                            "DATA_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "ATTR_TYPE_NAME", //label,
                            "ATTR_TYPE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "ATTR_SIZE", //label,
                            "ATTR_SIZE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "DECIMAL_DIGITS", //label,
                            "DECIMAL_DIGITS", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "NUM_PREC_RADIX", //label,
                            "NUM_PREC_RADIX", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            2, //displaySize,
                            "NULLABLE", //label,
                            "NULLABLE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "REMARKS", //label,
                            "REMARKS", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "ATTR_DEF", //label,
                            "ATTR_DEF", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "SQL_DATA_TYPE", //label,
                            "SQL_DATA_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "SQL_DATETIME_SUB", //label,
                            "SQL_DATETIME_SUB", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "CHAR_OCTET_LENGTH", //label,
                            "CHAR_OCTET_LENGTH", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            12, //displaySize,
                            "ORDINAL_POSITION", //label,
                            "ORDINAL_POSITION", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            12, //displaySize,
                            "IS_NULLABLE", //label,
                            "IS_NULLABLE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "SCOPE_CATALOG", //label,
                            "SCOPE_CATALOG", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "SCOPE_SCHEMA", //label,
                            "SCOPE_SCHEMA", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            120, //displaySize,
                            "SCOPE_TABLE", //label,
                            "SCOPE_TABLE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "SOURCE_DATA_TYPE", //label,
                            "SOURCE_DATA_TYPE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .build();
        }
        return attributesColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildPrimaryKeysColumnMetaData(
            final String schemaName) {
        if (primaryKeysColumnMetaData == null) {
            // 1. TABLE_CAT String => table catalog (may be null)
            // 2. TABLE_SCHEM String => table schema (may be null)
            // 3. TABLE_NAME String => table name
            // 4. COLUMN_NAME String => column name
            // 5. KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
            // 6. PK_NAME String => primary key name (may be null)
            int ordinal = 0;
            primaryKeysColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //autoIncrement,
                            true, //caseSensitive,
                            true, //searchable,
                            false, //currency,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_CAT", //label,
                            "TABLE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            "", //tableName,
                            "", //catalogName,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            true, //readOnly,
                            false, //writable,
                            false, //definitelyWritable,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //autoIncrement,
                            true, //caseSensitive,
                            true, //searchable,
                            false, //currency,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "TABLE_SCHEM", //label,
                            "TABLE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            "", //tableName,
                            "", //catalogName,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            true, //readOnly,
                            false, //writable,
                            false, //definitelyWritable,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //autoIncrement,
                            true, //caseSensitive,
                            true, //searchable,
                            false, //currency,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            120, //displaySize,
                            "TABLE_NAME", //label,
                            "TABLE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            "", //tableName,
                            "", //catalogName,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            true, //readOnly,
                            false, //writable,
                            false, //definitelyWritable,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //autoIncrement,
                            true, //caseSensitive,
                            true, //searchable,
                            false, //currency,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "COLUMN_NAME", //label,
                            "COLUMN_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            "", //tableName,
                            "", //catalogName,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            true, //readOnly,
                            false, //writable,
                            false, //definitelyWritable,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //autoIncrement,
                            false, //caseSensitive,
                            true, //searchable,
                            false, //currency,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "KEY_SEQ", //label,
                            "KEY_SEQ", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            "", //tableName,
                            "", //catalogName,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            true, //readOnly,
                            false, //writable,
                            false, //definitelyWritable,
                            short.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            false, //autoIncrement,
                            false, //caseSensitive,
                            true, //searchable,
                            false, //currency,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PK_NAME", //label,
                            "PK_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            "", //tableName,
                            "", //catalogName,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            true, //readOnly,
                            false, //writable,
                            false, //definitelyWritable,
                            String.class.getName()) //columnClassName
                    )
                    .build();
        }
        return primaryKeysColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildImportedKeysColumnMetaData(
            final String schemaName) {
        if (importedKeysColumnMetaData == null) {
            //  1. PKTABLE_CAT String => primary key table catalog being imported (may be null)
            //  2. PKTABLE_SCHEM String => primary key table schema being imported (may be null)
            //  3. PKTABLE_NAME String => primary key table name being imported
            //  4. PKCOLUMN_NAME String => primary key column name being imported
            //  5. FKTABLE_CAT String => foreign key table catalog (may be null)
            //  6. FKTABLE_SCHEM String => foreign key table schema (may be null)
            //  7. FKTABLE_NAME String => foreign key table name
            //  8. FKCOLUMN_NAME String => foreign key column name
            //  9. KEY_SEQ short => sequence number within a foreign key
            //        (a value of 1 represents the first column of the foreign key, a value of 2
            //        would represent the second column within the foreign key).
            // 10. UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
            //        importedNoAction - do not allow update of primary key if it has been imported
            //        importedKeyCascade - change imported key to agree with primary key update
            //        importedKeySetNull - change imported key to NULL if its primary key has been updated
            //        importedKeySetDefault - change imported key to default values if its primary key has been updated
            //        importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
            // 11. DELETE_RULE short => What happens to the foreign key when primary is deleted.
            //        importedKeyNoAction - do not allow delete of primary key if it has been imported
            //        importedKeyCascade - delete rows that import a deleted key
            //        importedKeySetNull - change imported key to NULL if its primary key has been deleted
            //        importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
            //        importedKeySetDefault - change imported key to default if its primary key has been deleted
            // 12. FK_NAME String => foreign key name (may be null)
            // 13. PK_NAME String => primary key name (may be null)
            // 14. DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
            //        importedKeyInitiallyDeferred - see SQL92 for definition
            //        importedKeyInitiallyImmediate - see SQL92 for definition
            //        importedKeyNotDeferrable - see SQL92 for definition
            int ordinal = 0;
            importedKeysColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PKTABLE_CAT", //label,
                            "PKTABLE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PKTABLE_SCHEM", //label,
                            "PKTABLE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            120, //displaySize,
                            "PKTABLE_NAME", //label,
                            "PKTABLE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "PKCOLUMN_NAME", //label,
                            "PKCOLUMN_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "FKTABLE_CAT", //label,
                            "FKTABLE_CAT", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "FKTABLE_SCHEM", //label,
                            "FKTABLE_SCHEM", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            120, //displaySize,
                            "FKTABLE_NAME", //label,
                            "FKTABLE_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            true, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            false, //signed,
                            255, //displaySize,
                            "FKCOLUMN_NAME", //label,
                            "FKCOLUMN_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "KEY_SEQ", //label,
                            "KEY_SEQ", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "UPDATE_RULE", //label,
                            "UPDATE_RULE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "DELETE_RULE", //label,
                            "DELETE_RULE", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "FK_NAME", //label,
                            "FK_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            false, //signed,
                            64, //displaySize,
                            "PK_NAME", //label,
                            "PK_NAME", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal, // not incremented
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            12, //displaySize,
                            "DEFERRABILITY", //label,
                            "DEFERRABILITY", //columnName,
                            schemaName, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .build();
        }
        return importedKeysColumnMetaData;
    }

    static ImmutableList<JdbcColumnMetaData> buildTypeInfoColumnMetaData() {
        /**
         * Retrieves a description of all the data types supported by this database. They are ordered by DATA_TYPE and then by how closely the data type maps to the corresponding JDBC SQL type.
         * If the database supports SQL distinct types, then getTypeInfo() will return a single row with a TYPE_NAME of DISTINCT and a DATA_TYPE of Types.DISTINCT. If the database supports SQL structured types, then getTypeInfo() will return a single row with a TYPE_NAME of STRUCT and a DATA_TYPE of Types.STRUCT.
         *
         * If SQL distinct or structured types are supported, then information on the individual types may be obtained from the getUDTs() method.
         *
         * Each type description has the following columns:
         *
         * TYPE_NAME String => Type name
         * DATA_TYPE int => SQL data type from java.sql.Types
         * PRECISION int => maximum precision
         * LITERAL_PREFIX String => prefix used to quote a literal (may be null)
         * LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
         * CREATE_PARAMS String => parameters used in creating the type (may be null)
         * NULLABLE short => can you use NULL for this type.
         *  typeNoNulls - does not allow NULL values
         *  typeNullable - allows NULL values
         *  typeNullableUnknown - nullability unknown
         * CASE_SENSITIVE boolean=> is it case sensitive.
         * SEARCHABLE short => can you use "WHERE" based on this type:
         *  typePredNone - No support
         *  typePredChar - Only supported with WHERE .. LIKE
         *  typePredBasic - Supported except for WHERE .. LIKE
         *  typeSearchable - Supported for all WHERE ..
         * UNSIGNED_ATTRIBUTE boolean => is it unsigned.
         * FIXED_PREC_SCALE boolean => can it be a money value.
         * AUTO_INCREMENT boolean => can it be used for an auto-increment value.
         * LOCAL_TYPE_NAME String => localized version of type name (may be null)
         * MINIMUM_SCALE short => minimum scale supported
         * MAXIMUM_SCALE short => maximum scale supported
         * SQL_DATA_TYPE int => unused
         * SQL_DATETIME_SUB int => unused
         * NUM_PREC_RADIX int => usually 2 or 10
         * The PRECISION column represents the maximum column size that the server supports for the given datatype. For numeric data, this is the maximum precision. For character data, this is the length in characters. For datetime datatypes, this is the length in characters of the String representation (assuming the maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data types where the column size is not applicable.
         */
        if (typeInfoColumnMetaData == null) {
            int ordinal = 0;
            typeInfoColumnMetaData = ImmutableList.<JdbcColumnMetaData>builder()
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "TYPE_NAME", //label,
                            "TYPE_NAME", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "DATA_TYPE", //label,
                            "DATA_TYPE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "PRECISION", //label,
                            "PRECISION", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "LITERAL_PREFIX", //label,
                            "LITERAL_PREFIX", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "LITERAL_SUFFIX", //label,
                            "LITERAL_SUFFIX", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "CREATE_PARAMS", //label,
                            "CREATE_PARAMS", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "NULLABLE", //label,
                            "NULLABLE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "CASE_SENSITIVE", //label,
                            "CASE_SENSITIVE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.BOOLEAN, //type.id,
                            JdbcType.BOOLEAN.name(), //type.name,
                            boolean.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "SEARCHABLE", //label,
                            "SEARCHABLE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "UNSIGNED_ATTRIBUTE", //label,
                            "UNSIGNED_ATTRIBUTE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.BOOLEAN, //type.id,
                            JdbcType.BOOLEAN.name(), //type.name,
                            boolean.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "FIXED_PREC_SCALE", //label,
                            "FIXED_PREC_SCALE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.BOOLEAN, //type.id,
                            JdbcType.BOOLEAN.name(), //type.name,
                            boolean.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "AUTO_INCREMENT", //label,
                            "AUTO_INCREMENT", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.BOOLEAN, //type.id,
                            JdbcType.BOOLEAN.name(), //type.name,
                            boolean.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNullable, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "LOCAL_TYPE_NAME", //label,
                            "LOCAL_TYPE_NAME", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.VARCHAR, //type.id,
                            JdbcType.VARCHAR.name(), //type.name,
                            String.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "MINIMUM_SCALE", //label,
                            "MINIMUM_SCALE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "MAXIMUM_SCALE", //label,
                            "MAXIMUM_SCALE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.SMALLINT, //type.id,
                            JdbcType.SMALLINT.name(), //type.name,
                            short.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "SQL_DATA_TYPE", //label,
                            "SQL_DATA_TYPE", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal++,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "SQL_DATETIME_SUB", //label,
                            "SQL_DATETIME_SUB", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .add(new JdbcColumnMetaData(
                            ordinal,
                            false, //caseSensitive,
                            ResultSetMetaData.columnNoNulls, //nullable,
                            true, //signed,
                            64, //displaySize,
                            "NUM_PREC_RADIX", //label,
                            "NUM_PREC_RADIX", //columnName,
                            null, //schemaName,
                            0, //precision,
                            0, //scale,
                            Types.INTEGER, //type.id,
                            JdbcType.INTEGER.name(), //type.name,
                            int.class.getName()) //columnClassName
                    )
                    .build();
        }
        return typeInfoColumnMetaData;
    }

}
