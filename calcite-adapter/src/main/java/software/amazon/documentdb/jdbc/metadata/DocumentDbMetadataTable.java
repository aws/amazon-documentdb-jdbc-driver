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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/** Represents the fields in a document, embedded document or array. */
@Getter
public class DocumentDbMetadataTable extends DocumentDbSchemaTable {

    private ImmutableMap<String, DocumentDbSchemaColumn> columnsByPath;

    @Getter(AccessLevel.NONE)
    private final ImmutableList<DocumentDbSchemaColumn> foreignKeys;

    /**
     * Builder for DocumentDbMetadataTable
     * @param sqlName The name of the table.
     * @param collectionName The name of the collection to which this table belongs.
     * @param columns The columns in this table, indexed by name. Uses LinkedHashMap to preserve order.
     * @param columnsByPath A map of columns indexed by path.
     * @param foreignKeys The foreign keys within the table.
     */
    @Builder
    public DocumentDbMetadataTable(final String sqlName,
                                   final String collectionName,
                                   final LinkedHashMap<String, DocumentDbSchemaColumn> columns,
                                   final ImmutableMap<String, DocumentDbMetadataColumn> columnsByPath,
                                   final ImmutableList<DocumentDbSchemaColumn> foreignKeys) {
        super(sqlName, collectionName, columns);
        this.foreignKeys = foreignKeys;
    }

    /**
     * The columns mapped by (non-empty) path.
     *
     * @return the map of path to {@link DocumentDbMetadataColumn}.
     */
    public ImmutableMap<String, DocumentDbSchemaColumn> getColumnsByPath() {
        if (columnsByPath == null) {
            final ImmutableMap.Builder<String, DocumentDbSchemaColumn> builder =
                    ImmutableMap.builder();
            for (Entry<String, DocumentDbSchemaColumn> entry : getColumns().entrySet()) {
                final DocumentDbSchemaColumn column = entry.getValue();
                if (!isNullOrWhitespace(column.getFieldPath())) {
                    builder.put(entry.getValue().getFieldPath(), entry.getValue());
                }
            }
            columnsByPath = builder.build();
        }
        return columnsByPath;
    }
}
