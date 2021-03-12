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

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Map.Entry;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/** Represents the fields in a document, embedded document or array. */
@Builder
@Getter
public class DocumentDbMetadataTable {

    /** The original path to the collection (if the base table) or field (if a virtual table). */
    private final String path;

    /** The columns the table is composed of indexed by their path. */
    private final ImmutableMap<String, DocumentDbMetadataColumn> columns;

    /** The display name of the table. */
    @Setter private String name;

    private ImmutableMap<String, DocumentDbMetadataColumn> columnsByPath;

    /**
     * The columns mapped by (non-empty) path.
     *
     * @return the map of path to {@link DocumentDbMetadataColumn}.
     */
    public ImmutableMap<String, DocumentDbMetadataColumn> getColumnsByPath() {
        if (columnsByPath == null) {
            // TODO: Remove after implementing JOIN properly.
            //  Added for the bad join that "merges" 2 tables into a new metadata table,
            //  creating duplicate columns. Not good :(.
            final HashSet<String> keys = new HashSet<>();
            final ImmutableMap.Builder<String, DocumentDbMetadataColumn> builder =
                    ImmutableMap.builder();
            for (Entry<String, DocumentDbMetadataColumn> entry : columns.entrySet()) {
                final DocumentDbMetadataColumn column = entry.getValue();
                if (!isNullOrWhitespace(column.getPath()) && !keys.contains(column.getPath())) {
                    keys.add(entry.getValue().getPath());
                    builder.put(entry.getValue().getPath(), entry.getValue());
                }
            }
            columnsByPath = builder.build();
        }
        return columnsByPath;
    }
}
