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
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

@Getter
public class DocumentDbSchemaTable {

    /**
     * The display name of the table.
     */
    @Setter
    @NonNull
    private String sqlName;

    /**
     * The name of the DocumentDB collection.
     */
    @NonNull
    private final String collectionName;

    /**
     * The time the metadata was created or updated.
     */
    private final Date modifyDate;

    /**
     * The list of columns in the table.
     */
    @NonNull
    private final ImmutableMap<String, DocumentDbSchemaColumn> columns;

    /**
     * Basic all argument constructor for table.
     *
     * @param sqlName        The name of the table.
     * @param collectionName The DocumentDB collection name.
     * @param columns        The columns contained in the table indexed by name. Uses LinkedHashMap to preserve order.
     */
    public DocumentDbSchemaTable(final String sqlName, final String collectionName,
                                 final LinkedHashMap<String, DocumentDbSchemaColumn> columns) {
        this.sqlName = sqlName;
        this.collectionName = collectionName;
        this.modifyDate = new Date(Instant.now().toEpochMilli());
        this.columns = ImmutableMap.copyOf(columns);
    }

    /**
     * The columns that are foreign keys.
     *
     * @return the foreign keys as a list of {@link DocumentDbMetadataColumn}.
     */
    public ImmutableList<DocumentDbSchemaColumn> getForeignKeys() {
        return ImmutableList.copyOf(getColumns().values()
                .stream()
                .filter(entry -> entry.getForeignKeyTableName() != null)
                .collect(Collectors.toList()));
    }

    public Date getModifyDate() {
        return new Date(modifyDate.getTime());
    }
}
