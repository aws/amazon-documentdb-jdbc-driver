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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.bson.BsonType;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;
import software.amazon.documentdb.jdbc.common.utilities.JdbcType;

import java.util.Objects;
import java.util.Optional;

@Getter
@JsonSerialize(as = DocumentDbSchemaColumn.class)
public class DocumentDbSchemaColumn {

    /** Original path to the field in the collection. */
    @NonNull
    @BsonProperty("fieldPath")
    @JsonProperty("fieldPath")
    private final String fieldPath;

    /** Display name of the field. */
    @Setter
    @NonNull
    @BsonProperty("sqlName")
    @JsonProperty("sqlName")
    private String sqlName;

    /** SQL/JDBC type of the field. Refer to the types in {@link java.sql.Types} */
    @Setter
    @BsonProperty("sqlType")
    @JsonProperty("sqlType")
    private JdbcType sqlType;

    /** The DocumentDB type of the field. Refer to the types in {@link org.bson.BsonType} */
    @BsonProperty("dbType")
    @JsonProperty("dbType")
    private final BsonType dbType;

    /**
     * {@code true} if this column is the index column in an array table;
     * {@code false} otherwise.
     */
    @BsonProperty("isIndex")
    @JsonProperty("isIndex")
    private final boolean index;

    /**
     * {@code true} if this column is part of the primary key;
     * {@code false} otherwise.
     */
    @BsonProperty("isPrimaryKey")
    @JsonProperty("isPrimaryKey")
    private final boolean primaryKey;

    /** If this column is a foreign key this contains the name of the table that it refers to, null otherwise. */
    @Setter
    @BsonProperty("foreignKeyTableName")
    @JsonProperty("foreignKeyTableName")
    private String foreignKeyTableName;

    /** If this column is a foreign key this contains the name of the column that it refers to, null otherwise. */
    @Setter
    @BsonProperty("foreignKeyColumnName")
    @JsonProperty("foreignKeyColumnName")
    private String foreignKeyColumnName;

    /**
     * All-args constructor for a column.
     *
     * @param fieldPath The path to this column.
     * @param sqlName The name of this column.
     * @param sqlType The SQL/JDBC type of this column.
     * @param dbType The DocumentDB type of this column. (Optional)
     * @param isIndex Whether this is an index column. (Optional)
     * @param primaryKey Whether this is part of a primary key. (Optional)
     * @param foreignKeyTableName If this is a foreign key, the table that it refers to, null if not a foreign key.
     * @param foreignKeyColumnName If this is a foreign key, the column that it refers to, null if not a foreign key.
     */
    @BsonCreator
    public DocumentDbSchemaColumn(
            @JsonProperty("fieldPath") @BsonProperty("fieldPath")
            final String fieldPath,
            @JsonProperty("sqlName") @BsonProperty("sqlName")
            final String sqlName,
            @JsonProperty("sqlType") @BsonProperty("sqlType")
            final JdbcType sqlType,
            @JsonProperty("dbType") @BsonProperty("dbType")
            final BsonType dbType,
            @JsonProperty("isIndex") @BsonProperty("isIndex")
            final boolean isIndex,
            @JsonProperty("isPrimaryKey") @BsonProperty("isPrimaryKey")
            final boolean primaryKey,
            @JsonProperty("foreignKeyTableName") @BsonProperty("foreignKeyTableName")
            final String foreignKeyTableName,
            @JsonProperty("foreignKeyColumnName") @BsonProperty("foreignKeyColumnName")
            final String foreignKeyColumnName) {
        this.fieldPath = fieldPath;
        this.sqlName = sqlName;
        this.sqlType = sqlType;
        this.dbType = dbType;
        this.index = isIndex;
        this.primaryKey = primaryKey;
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
        Integer colIndex = 0;
        for (DocumentDbSchemaColumn column: table.getColumns().values()) {
            colIndex++;
            if (column.getSqlName().equals(this.getSqlName())) {
                return Optional.of(colIndex);
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentDbSchemaColumn)) {
            return false;
        }
        final DocumentDbSchemaColumn that = (DocumentDbSchemaColumn) o;
        return index == that.index
                && primaryKey == that.primaryKey
                && fieldPath.equals(that.fieldPath)
                && sqlName.equals(that.sqlName)
                && sqlType == that.sqlType
                && dbType == that.dbType
                && Objects.equals(foreignKeyTableName, that.foreignKeyTableName)
                && Objects.equals(foreignKeyColumnName, that.foreignKeyColumnName);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(fieldPath, sqlName, sqlType, dbType, index, primaryKey, foreignKeyTableName,
                        foreignKeyColumnName);
    }
}
