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

package software.amazon.documentdb.jdbc.common.utilities;

import java.util.HashMap;
import java.util.Map;

/**
 * Copy of the java.sql.Types constants but as an enum, for use in lookups.
 * Warning: if a JDBC type is added or deprecated, the change should be reflected
 * on the ODBC driver as well. Files to be changed in the ODBC driver:
 * src\binary\include\ignite\impl\binary\binary_common.h, and
 * functions BinaryToSqlTypeName, BinaryToSqlType and SqlTypeToBinary
 * in src\odbc\src\type_traits.cpp
 */
public enum JdbcType {
    BIT(-7),
    TINYINT(-6),
    SMALLINT(5),
    INTEGER(4),
    BIGINT(-5),
    FLOAT(6),
    REAL(7),
    DOUBLE(8),
    NUMERIC(2),
    DECIMAL(3),
    CHAR(1),
    VARCHAR(12),
    LONGVARCHAR(-1),
    DATE(91),
    TIME(92),
    TIMESTAMP(93),
    BINARY(-2),
    VARBINARY(-3),
    LONGVARBINARY(-4),
    BLOB(2004),
    CLOB(2005),
    BOOLEAN(16),
    ARRAY(2003),
    STRUCT(2002),
    JAVA_OBJECT(2000),
    ROWID(-8),
    NCHAR(-15),
    NVARCHAR(-9),
    LONGNVARCHAR(-16),
    NCLOB(2011),
    SQLXML(2009),
    REF_CURSOR(2012),
    NULL(0);

    private static final Map<Integer, JdbcType> TYPE_MAP = new HashMap<>();

    /**
     * The java.sql.Types JDBC type.
     */
    private final int jdbcType;

    static {
        for (JdbcType type : JdbcType.values()) {
            TYPE_MAP.put(type.jdbcType, type);
        }
    }

    /**
     * JdbcType constructor.
     * @param jdbcType The java.sql.Types JDBC type associated with this value.
     */
    JdbcType(final int jdbcType) {
        this.jdbcType = jdbcType;
    }

    /**
     * Get the JDBC type.
     *
     * @return an integer for the JDBC type.
     */
    public int getJdbcType() {
        return jdbcType;
    }

    /**
     * Get the type associated with the JDBC type.
     *
     * @param type the type value to search for.
     *
     * @return a {@link JdbcType} assocated with the JDBC type value.
     */
    public static JdbcType fromType(final int type) {
        return TYPE_MAP.get(type);
    }
}

