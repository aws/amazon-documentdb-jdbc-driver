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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.LazyLinkedHashMap;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;

import javax.annotation.Nullable;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSchema.class);
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(Include.NON_NULL)
            .setSerializationInclusion(Include.NON_EMPTY)
            .enable(SerializationFeature.INDENT_OUTPUT);
    private static final String EMPTY_STRING = "";

    public static final String DEFAULT_SCHEMA_NAME = "_default";

    /**
     * The name of the schema.
     */
    @NonNull
    @Setter
    @BsonProperty("schemaName")
    private String schemaName;

    /**
     * The version number of this metadata.
     */
    @Setter
    @BsonProperty("schemaVersion")
    private int schemaVersion;

    /**
     * The name of the database, same as the DocumentDB database by default.
     */
    @NonNull
    @BsonProperty("sqlName")
    private final String sqlName;

    /**
     * The time this metadata was created or updated.
     */
    @NonNull
    @BsonProperty("modifyDate")
    private Date modifyDate;

    /**
     * The map of schema tables.
     */
    @Getter(AccessLevel.NONE)
    @Nullable
    @BsonIgnore
    @JsonIgnore
    private Map<String, DocumentDbSchemaTable> tables;

    @Nullable
    @BsonIgnore
    @JsonIgnore
    public Map<String, DocumentDbSchemaTable> getTableMap() {
        return tables;
    }

    /**
     * The list of table references.
     */
    @BsonProperty("tables")
    @JsonProperty("tables")
    private final Set<String> tableReferences;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Function<String, DocumentDbSchemaTable> getTableFunction;

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
            @NonNull final Function<String, DocumentDbSchemaTable> getTableFunction)
            throws IllegalStateException, DocumentDbSchemaException {
        if (this.getTableFunction != null || this.tables != null
                || this.tableReferences == null) {
            throw new IllegalStateException(
                    SqlError.lookup(SqlError.INVALID_STATE_SET_TABLE_FUNCTION));
        }
        this.getTableFunction = getTableFunction;
        final Map<String, String> tableIdByTableName = new LinkedHashMap<>();
        final LinkedHashSet<String> tableNames = new LinkedHashSet<>(); this.tableReferences.stream()
                .map(t -> t.split("[:][:]")[0])
                .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String tableId : this.tableReferences) {
            final String[] tableNameAndUuid = tableId.split("[:][:]");
            if (tableNameAndUuid.length != 2) {
                throw new DocumentDbSchemaException(
                        SqlError.lookup(SqlError.INVALID_FORMAT,
                                tableId, "<tableName>::<tableId>"));
            }
            final String tableName = tableNameAndUuid[0];
            tableIdByTableName.put(tableName, tableId);
            tableNames.add(tableName);
        }
        this.tables = new LazyLinkedHashMap<>(
                tableNames,
                tableName -> getTableFunction.apply(tableIdByTableName.get(tableName)));
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
            @JsonProperty("schemaName") @BsonProperty("schemaName") final String schemaName,
            @JsonProperty("schemaVersion") @BsonProperty("schemaVersion") final int schemaVersion,
            @JsonProperty("sqlName") @BsonProperty("sqlName") final String sqlName,
            @JsonProperty("modifyDate") @BsonProperty("modifyDate") final Date modifyDate,
            @JsonProperty("tables") @BsonProperty("tables") final Set<String> tableReferences) {
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

    /**
     * Set the lasted modify date.
     *
     * @param modifyDate the last modified date.
     */
    public void setModifyDate(final Date modifyDate) {
        this.modifyDate = new Date(modifyDate.getTime());
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
