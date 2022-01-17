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
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.LazyLinkedHashMap;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@JsonSerialize(as = DocumentDbSchema.class)
public class DocumentDbSchema {

    public static final String SCHEMA_NAME_PROPERTY = "schemaName";
    public static final String SCHEMA_VERSION_PROPERTY = "schemaVersion";
    public static final String SQL_NAME_PROPERTY = "sqlName";
    public static final String ID_PROPERTY = "_id";
    public static final String MODIFY_DATE_PROPERTY = "modifyDate";
    public static final String TABLES_PROPERTY = "tables";
    public static final String SCHEMA_TABLE_ID_SEPARATOR = "::";
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSchema.class);
    private static final ObjectMapper JSON_OBJECT_MAPPER = JsonMapper.builder()
            .serializationInclusion(Include.NON_NULL)
            .serializationInclusion(Include.NON_EMPTY)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .build();
    private static final String EMPTY_STRING = "";

    public static final String DEFAULT_SCHEMA_NAME = "_default";

    /**
     * The name of the schema.
     */
    @NonNull
    @Setter
    @BsonProperty(SCHEMA_NAME_PROPERTY)
    private String schemaName;

    /**
     * The version number of this metadata.
     */
    @Setter
    @BsonProperty(SCHEMA_VERSION_PROPERTY)
    private int schemaVersion;

    /**
     * The name of the database, same as the DocumentDB database by default.
     */
    @NonNull
    @BsonProperty(SQL_NAME_PROPERTY)
    private final String sqlName;

    /**
     * The time this metadata was created or updated.
     */
    @NonNull
    @BsonProperty(MODIFY_DATE_PROPERTY)
    private final Date modifyDate;

    /**
     * The map of schema tables.
     */
    @Getter(AccessLevel.NONE)
    @BsonIgnore
    @JsonIgnore
    private Map<String, DocumentDbSchemaTable> tables;

    @BsonIgnore
    @JsonIgnore
    public Map<String, DocumentDbSchemaTable> getTableMap() {
        return tables;
    }

    /**
     * The list of table references.
     */
    @BsonProperty(TABLES_PROPERTY)
    @JsonProperty(TABLES_PROPERTY)
    private final Set<String> tableReferences;

    /**
     * Sets the lazy load function for table schema retrieval.
     *
     * @param getTableFunction the function to retrieve table schema using the table ID as
     *                         the input parameter to the lambda function.
     * @throws IllegalStateException if the function is already set or the #tables collection
     * is already set.
     */
    @BsonIgnore
    @JsonIgnore
    public void setGetTableFunction(
            @NonNull final Function<String, DocumentDbSchemaTable> getTableFunction,
            @NonNull final Function<Set<String>, Map<String, DocumentDbSchemaTable>> getRemainingTablesFunction)
            throws IllegalStateException {
        if (this.tables != null || this.tableReferences == null) {
            throw new IllegalStateException(
                    SqlError.lookup(SqlError.INVALID_STATE_SET_TABLE_FUNCTION));
        }
        final Map<String, String> tableIdByTableName = this.tableReferences.stream()
                .collect(Collectors.toMap(
                        DocumentDbSchema::parseSqlTableName,
                        tableId -> tableId,
                        (a, b) -> b,
                        LinkedHashMap::new));
        this.tables = new LazyLinkedHashMap<>(
                new LinkedHashSet<>(tableIdByTableName.keySet()),
                tableName -> getTableFunction
                        .apply(tableIdByTableName.get(tableName)),
                remainingTableNames -> getRemainingTablesFunction
                        .apply(tableIdByTableName.keySet().stream()
                                .filter(remainingTableNames::contains)
                                .map(tableIdByTableName::get)
                                .collect(Collectors.toCollection(LinkedHashSet::new))));
    }

    /**
     * Parses the SQL table name from the given table ID.
     *
     * @param tableId the table ID to parse.
     *
     * @return a SQL table name.
     */
    public static String parseSqlTableName(final String tableId) {
        return parseTableNameAndUuid(tableId)[0];
    }

    @SneakyThrows
    private static String[] parseTableNameAndUuid(final String tableId) {
        final String[] tableNameAndUuid = tableId.split("[:][:]");
        if (tableNameAndUuid.length != 2) {
            throw new DocumentDbSchemaException(
                    SqlError.lookup(SqlError.INVALID_FORMAT,
                            tableId, "<tableName>::<tableId>"));
        }
        return tableNameAndUuid;
    }

    /**
     * All args constructor for collection metadata.
     * @param sqlName Name of the collection.
     * @param schemaVersion Version of this metadata.
     */
    public DocumentDbSchema(
            final String sqlName,
            final int schemaVersion,
            final Map<String, DocumentDbSchemaTable> tables) {
        this(DEFAULT_SCHEMA_NAME, sqlName, schemaVersion, tables);
    }

    /**
     * All args constructor for collection metadata.
     * @param schemaName the name of the schema.
     * @param sqlName the name of SQL table
     * @param schemaVersion Version of this metadata.
     */
    public DocumentDbSchema(
            final String schemaName,
            final String sqlName,
            final int schemaVersion,
            final Map<String, DocumentDbSchemaTable> tables) {
        this.schemaName = schemaName;
        this.sqlName = sqlName;
        this.schemaVersion = schemaVersion;
        this.modifyDate = new Date(Instant.now().toEpochMilli());
        this.tableReferences = tables.values().stream()
                .map(DocumentDbSchemaTable::getId)
                .collect(Collectors.toSet());
        this.tables = tables;
    }

    /**
     * Creates in instance of {@link DocumentDbSchema}. Used for reading/writing
     * to the persistent storage.
     *  @param schemaName the name of the schema.
     * @param schemaVersion the version of the schema.
     * @param sqlName the name of the database or collection.
     * @param modifyDate the last modified date of the schema.
     */
    @BsonCreator
    @JsonCreator
    public DocumentDbSchema(
            @JsonProperty(SCHEMA_NAME_PROPERTY) @BsonProperty(SCHEMA_NAME_PROPERTY) final String schemaName,
            @JsonProperty(SCHEMA_VERSION_PROPERTY) @BsonProperty(SCHEMA_VERSION_PROPERTY) final int schemaVersion,
            @JsonProperty(SQL_NAME_PROPERTY) @BsonProperty(SQL_NAME_PROPERTY) final String sqlName,
            @JsonProperty(MODIFY_DATE_PROPERTY) @BsonProperty(MODIFY_DATE_PROPERTY) final Date modifyDate,
            @JsonProperty(TABLES_PROPERTY) @BsonProperty(TABLES_PROPERTY) final Set<String> tableReferences) {
        this.schemaName = schemaName;
        this.sqlName = sqlName;
        this.schemaVersion = schemaVersion;
        this.modifyDate = new Date(modifyDate.getTime());
        // TODO: Use this to setup the LazyLinkedHashMap for the tables map.
        this.tableReferences = tableReferences != null ? tableReferences : new LinkedHashSet<>();
    }

    /**
     * Creates and returns the time the metadata was created or updated.
     *
     * @return a copy of the Date of modification.
     */
    public Date getModifyDate() {
        return new Date(modifyDate.getTime());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentDbSchema)) {
            return false;
        }
        final DocumentDbSchema that = (DocumentDbSchema) o;
        return schemaVersion == that.schemaVersion
                && schemaName.equals(that.schemaName)
                && sqlName.equals(that.sqlName)
                && modifyDate.equals(that.modifyDate)
                && Objects.equals(tableReferences, that.tableReferences);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, schemaVersion, sqlName, modifyDate, tableReferences);
    }

    @Override
    public String toString() {
        try {
            return JSON_OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error converting object to JSON.", e);
        }
        return EMPTY_STRING;
    }
}
