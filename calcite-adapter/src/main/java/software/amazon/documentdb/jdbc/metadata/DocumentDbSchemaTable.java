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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.ID_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.MODIFY_DATE_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SCHEMA_TABLE_ID_SEPARATOR;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SQL_NAME_PROPERTY;

@Getter
@JsonSerialize(as = DocumentDbSchemaTable.class)
public class DocumentDbSchemaTable {

    public static final String UUID_PROPERTY = "uuid";
    public static final String COLLECTION_NAME_PROPERTY = "collectionName";
    public static final String COLUMNS_PROPERTY = "columns";

    /**
     * The unique ID for the table schema.
     *
     * @return a {@link String} representing the combination of {@link #getSqlName} and
     * {@link #getUuid}.
     */
    @BsonId
    @JsonProperty(ID_PROPERTY)
    public String getId() {
        return getSchemaId(sqlName, uuid);
    }

    private static String getSchemaId(final String sqlName, final String uuid) {
        return sqlName + SCHEMA_TABLE_ID_SEPARATOR + uuid;
    }

    /**
     * The schema's unique ID.
     */
    @Setter
    private String uuid;

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
    @BsonIgnore
    @JsonIgnore
    private final ImmutableMap<String, DocumentDbSchemaColumn> columnMap;

    @JsonProperty(COLUMNS_PROPERTY)
    @BsonProperty(COLUMNS_PROPERTY)
    private final List<DocumentDbSchemaColumn> columns;

    @Setter
    @BsonIgnore
    @JsonIgnore
    private long estimatedRecordCount = -1;

    /**
     * Creates an instance from deserializing a document.
     *
     * @param uuid the version of the table schema.
     * @param modifyDate the last modified date of the schema.
     * @param sqlName the SQL name of the table.
     * @param collectionName the reference collection.
     * @param columns the list of columns in the schema.
     */
    @BsonCreator
    @JsonCreator
    public DocumentDbSchemaTable(
            @JsonProperty(ID_PROPERTY) @BsonId
            final String id,
            @JsonProperty(UUID_PROPERTY) @BsonProperty(UUID_PROPERTY)
            final String uuid,
            @JsonProperty(MODIFY_DATE_PROPERTY) @BsonProperty(MODIFY_DATE_PROPERTY)
            final Date modifyDate,
            @JsonProperty(SQL_NAME_PROPERTY) @BsonProperty(SQL_NAME_PROPERTY)
            final String sqlName,
            @JsonProperty(COLLECTION_NAME_PROPERTY) @BsonProperty(COLLECTION_NAME_PROPERTY)
            final String collectionName,
            @JsonProperty(COLUMNS_PROPERTY) @BsonProperty(COLUMNS_PROPERTY)
            final List<DocumentDbSchemaColumn> columns) {

        this.uuid = !Strings.isNullOrEmpty(uuid) ? uuid : UUID.randomUUID().toString();
        this.modifyDate = new Date(modifyDate.getTime());
        this.sqlName = sqlName;
        this.collectionName = collectionName;
        this.columns = columns;
        final LinkedHashMap<String, DocumentDbSchemaColumn> map = columns.stream()
                .collect(Collectors.toMap(
                        DocumentDbSchemaColumn::getSqlName,
                        documentDbSchemaColumn -> documentDbSchemaColumn,
                        (original, duplicate) -> original, // Ignore duplicates
                        LinkedHashMap::new));
        this.columnMap = ImmutableMap.copyOf(map);
    }

    /**
     * Basic all argument constructor for table.
     *
     * @param sqlName        The name of the table.
     * @param collectionName The DocumentDB collection name.
     * @param columnMap        The columns contained in the table indexed by name. Uses LinkedHashMap to preserve order.
     */
    public DocumentDbSchemaTable(final String sqlName,
            final String collectionName,
            final Map<String, DocumentDbSchemaColumn> columnMap) {
        this.uuid = UUID.randomUUID().toString();
        this.modifyDate = new Date(Instant.now().toEpochMilli());
        this.sqlName = sqlName;
        this.collectionName = collectionName;
        this.columns = new ArrayList<>(columnMap.values());
        this.columnMap = ImmutableMap.copyOf(columnMap);
 }

    /**
     * The columns that are foreign keys.
     *
     * @return the foreign keys as a list of {@link DocumentDbMetadataColumn}.
     */
    public ImmutableList<DocumentDbSchemaColumn> getForeignKeys() {
        return ImmutableList.copyOf(getColumnMap().values()
                .stream()
                .filter(entry -> entry.getForeignKeyTableName() != null)
                .collect(Collectors.toList()));
    }

    public Date getModifyDate() {
        return new Date(modifyDate.getTime());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentDbSchemaTable)) {
            return false;
        }
        final DocumentDbSchemaTable that = (DocumentDbSchemaTable) o;
        return Objects.equals(uuid, that.uuid)
                && sqlName.equals(that.sqlName)
                && collectionName.equals(that.collectionName)
                && Objects.equals(modifyDate, that.modifyDate)
                && columnMap.equals(that.columnMap)
                && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, sqlName, collectionName, modifyDate, columnMap, columns);
    }
}
