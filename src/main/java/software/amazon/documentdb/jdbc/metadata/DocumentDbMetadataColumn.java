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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import org.bson.BsonType;
import software.amazon.documentdb.jdbc.common.utilities.JdbcType;

import java.util.Objects;

/** Represents a field in a document, embedded document or array as a column in a table. */
@Getter
@JsonSerialize(as = DocumentDbSchemaColumn.class)
public class DocumentDbMetadataColumn extends DocumentDbSchemaColumn {

    /** The (one-indexed) index of the column in the table. */
    private int index;

    /**
     * Indicates the position of the column in the primary key of the table; 0 if not part of the key.
     */
    private int primaryKeyIndex;

    /**
     * Indicates the position of the column in the foreign key of the table; 0 if not part of the key.
     */
    private int foreignKeyIndex;

    /** If this column is an array index, returns the zero-indexed level of the array. Null, otherwise. */
    private final Integer arrayIndexLevel;

    /** The name of the table this column belongs. */
    @Getter(AccessLevel.PACKAGE)
    private final String tableName;

    private final String virtualTableName;

    /** The new path of a column that was renamed due to a path collision. Only used in query processing. **/
    private final String resolvedPath;

    /**
     * {@code true} if the column was generated rather than taken directly from the collection;
     * {@code false} otherwise.
     */
    private final boolean isGenerated;

    /**
     * Builder for DocumentDbMetadataColumn
     *
     * @param index The index of the column within the table (one-indexed).
     * @param primaryKeyIndex If this key is part of primary key, the index within the primary key; else zero.
     * @param foreignKeyIndex If this key is part of a foreign key, the index within the foreign key; else zero.
     * @param arrayIndexLevel The level of the array if column is the index column of an array.
     * @param virtualTableName The name of the virtual table this column belongs to.
     * @param tableName The name of the table this column belongs to.
     * @param resolvedPath The modified path if this column is renamed due to a collision in joins.
     * @param isGenerated Whether this column was generated.
     * @param fieldPath The path to this field.
     * @param sqlName The name of the column.
     * @param sqlType The SQL/JDBC type, see {@link java.sql.Types}.
     * @param dbType The DocumentDB type, see {@link BsonType}
     * @param isIndex Whether this column is an array index column.
     * @param isPrimaryKey Whether this column is part of the primary key.
     * @param foreignKeyTableName The name of the table referred to if this column is part of a
     *                            foreign key.
     * @param foreignKeyColumnName The name of the column referred to if this column is part of a
     *                             foreign key.
     */
    @Builder()
    public DocumentDbMetadataColumn(final int index,
                                    final int primaryKeyIndex,
                                    final int foreignKeyIndex,
                                    final Integer arrayIndexLevel,
                                    final String virtualTableName,
                                    final String tableName,
                                    final String resolvedPath,
                                    final boolean isGenerated,
                                    final String fieldPath,
                                    final String sqlName,
                                    final JdbcType sqlType,
                                    final BsonType dbType,
                                    final boolean isIndex,
                                    final boolean isPrimaryKey,
                                    final String foreignKeyTableName,
                                    final String foreignKeyColumnName) {
        super(fieldPath, sqlName, sqlType, dbType, isIndex, isPrimaryKey, foreignKeyTableName, foreignKeyColumnName);
        this.index = index;
        this.primaryKeyIndex = primaryKeyIndex;
        this.foreignKeyIndex = foreignKeyIndex;
        this.arrayIndexLevel = arrayIndexLevel;
        this.tableName = tableName;
        this.virtualTableName = virtualTableName;
        this.resolvedPath = resolvedPath;
        this.isGenerated = isGenerated;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentDbMetadataColumn)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final DocumentDbMetadataColumn that = (DocumentDbMetadataColumn) o;
        return index == that.index
                && primaryKeyIndex == that.primaryKeyIndex
                && foreignKeyIndex == that.foreignKeyIndex
                && isGenerated == that.isGenerated
                && Objects.equals(arrayIndexLevel, that.arrayIndexLevel)
                && Objects .equals(tableName, that.tableName)
                && Objects.equals(virtualTableName, that.virtualTableName)
                && Objects.equals(resolvedPath, that.resolvedPath);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(super.hashCode(), index, primaryKeyIndex, foreignKeyIndex, arrayIndexLevel,
                        tableName, virtualTableName, resolvedPath, isGenerated);
    }
}
