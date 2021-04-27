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
package software.amazon.documentdb.jdbc.metadata;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Optional;

@Getter
public class DocumentDbSchemaColumn {

    /** Original path to the field in the collection. */
    @NonNull
    private final String fieldPath;

    /** Display name of the field. */
    @Setter
    @NonNull
    private String sqlName;

    /** SQL/JDBC type of the field. Refer to the types in {@link java.sql.Types} */
    @Setter
    private int sqlType;

    /** The DocumentDB type of the field. Refer to the types in {@link org.bson.BsonType}*/
    private final int dbType;

    /**
     * {@code true} if this column is the index column in an array table;
     * {@code false} otherwise.
     */
    private final boolean isIndex;

    /**
     * {@code true} if this column is part of the primary key;
     * {@code false} otherwise.
     */
    private final boolean isPrimaryKey;

    /** If this column is a foreign key this contains the name of the table that it refers to, null otherwise. */
    @Setter
    private String foreignKeyTableName;

    /** If this column is a foreign key this contains the name of the column that it refers to, null otherwise. */
    @Setter
    private String foreignKeyColumnName;

    /**
     * All-args constructor for a column.
     *
     * @param fieldPath The path to this column.
     * @param sqlName The name of this column.
     * @param sqlType The SQL/JDBC type of this column.
     * @param dbType The DocumentDB type of this column. (Optional)
     * @param isIndex Whether this is an index column. (Optional)
     * @param isPrimaryKey Whether this is part of a primary key. (Optional)
     * @param foreignKeyTableName If this is a foreign key, the table that it refers to, null if not a foreign key.
     * @param foreignKeyColumnName If this is a foreign key, the column that it refers to, null if not a foreign key.
     */
    public DocumentDbSchemaColumn(final String fieldPath,
                                  final String sqlName,
                                  final int sqlType,
                                  final int dbType,
                                  final boolean isIndex,
                                  final boolean isPrimaryKey,
                                  final String foreignKeyTableName,
                                  final String foreignKeyColumnName) {
        this.fieldPath = fieldPath;
        this.sqlName = sqlName;
        this.sqlType = sqlType;
        this.dbType = dbType;
        this.isIndex = isIndex;
        this.isPrimaryKey = isPrimaryKey;
        this.foreignKeyTableName = foreignKeyTableName;
        this.foreignKeyColumnName = foreignKeyColumnName;
    }

    /**
     * Gets the index of the column within the given table.
     * @param table The parent table of this column.
     * @return the index of this column within the given table (one-indexed), will return empty optional otherwise if
     *      the table given does not contain this column.
     */
    public Optional<Integer> getIndex(final DocumentDbSchemaTable table) {
        Integer index = 0;
        for (DocumentDbSchemaColumn column: table.getColumns().values()) {
            index++;
            if (column.getSqlName().equals(this.getSqlName())) {
                return Optional.of(index);
            }
        }
        return Optional.empty(); // Column was not found in the given table.
    }


    /**
     * Gets the index of the column within the primary key of the given table.
     * @param table The parent table of this column.
     * @return the index of this column within the primary key (one-indexed), will return zero if this column is not
     * part of the primary key, or empty optional if it is not within the table given.
     */
    public Optional<Integer> getPrimaryKeyIndex(final DocumentDbSchemaTable table) {
        Integer keyIndex = 0;
        for (DocumentDbSchemaColumn column: table.getColumns().values()) {
            if (column.isPrimaryKey()) {
                keyIndex++;
                if (column.getSqlName().equals(this.getSqlName())) {
                    return Optional.of(keyIndex);
                }
            } else if (column.getSqlName().equals(this.getSqlName())) {
                return Optional.of(0); // Column is not part of primary key.
            }
        }
        return Optional.empty(); // Column was not found in this table.
    }

    /**
     * Gets the index of the column within a foreign key.
     * @param table The parent table of this column.
     * @return the index of this column within the foreign key (one-indexed), will return empty optional if is not
     * a foreign key to the given table.
     */
    public Optional<Integer> getForeignKeyIndex(final DocumentDbSchemaTable table) {
        if (table.getSqlName().equals(getForeignKeyTableName())) {
            Integer keyIndex = 0;
            for (DocumentDbSchemaColumn column : table.getColumns().values()) {
                if (column.isPrimaryKey()) {
                    keyIndex++;
                    if (column.getSqlName().equals(this.getForeignKeyColumnName())) {
                        return Optional.of(keyIndex);
                    }
                }
            }
        }
        return Optional.empty(); // Column was not found in this table.
    }
}
