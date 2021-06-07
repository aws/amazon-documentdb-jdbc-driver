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

package software.amazon.documentdb.jdbc.persist;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import lombok.NonNull;
import lombok.SneakyThrows;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class FileSchemaReader implements SchemaReader {
    public static final String DEFAULT_SCHEMA_NAME = DocumentDbSchema.DEFAULT_SCHEMA_NAME;
    static final String DEFAULT_FOLDER = ".documentdb";
    static final ObjectMapper OBJECT_MAPPER;

    private static final String INVALID_FILE_CHARACTERS_REGEX = "[/?%*:|\"<>\\\\]";
    private final Path schemaFolder;
    private final String databaseName;

    static {
        OBJECT_MAPPER = new ObjectMapper()
                .setDateFormat(new StdDateFormat().withColonInTimeZone(true))
                .setSerializationInclusion(Include.NON_NULL)
                .setSerializationInclusion(Include.NON_EMPTY)
                .setSerializationInclusion(Include.NON_DEFAULT)
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        OBJECT_MAPPER
                .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
                // Make the enums serialize to lower case.
                .registerModule(buildEnumLowerCaseSerializerModule())
                .registerModule(new GuavaModule()); // Immutable*
    }


    private static @NonNull SimpleModule buildEnumLowerCaseSerializerModule() {
        final SimpleModule module = new SimpleModule();
        final JsonSerializer<Enum<?>> serializer = new StdSerializer<Enum<?>>(Enum.class, true) {
            @Override
            public void serialize(final Enum value, final JsonGenerator jGen,
                    final SerializerProvider provider) throws IOException {
                jGen.writeString(value.name().toLowerCase());
            }
        };
        module.addSerializer(serializer);
        return module;
    }

    /**
     * Creates a new {@link FileSchemaReader} from a given SQL database name using the default
     * path of {@code ~/.documentdb/}.
     *
     * @param databaseName the SQL database name.
     */
    public FileSchemaReader(final String databaseName) {
        this(databaseName, Paths.get(System.getProperty("user.home"), DEFAULT_FOLDER));
    }

    /**
     * Creates a new {@link FileSchemaReader} from a given SQL database name and file system
     * folder path.
     *
     * @param databaseName the SQL database name.
     * @param schemaFolder the {@link Path} to the folder where schema is stored.
     */
    public FileSchemaReader(
            @NonNull final String databaseName,
            @NonNull final Path schemaFolder) {
        this.databaseName = databaseName;
        this.schemaFolder = schemaFolder;
    }

    @SneakyThrows
    static @NonNull File getSchemaFile(
            @NonNull final String schemaName,
            @NonNull final String databaseName,
            final Path schemaFolder) {
        final File folder = schemaFolder.toFile();
        if (!folder.exists()) {
            if (!folder.mkdirs()) {
                throw new IOException(SqlError.lookup(SqlError.CREATE_FOLDER_FAILED, folder));
            }
        }

        // Replace /, ?, %, *, :, |, ", <, >, or \ with underscore.
        final String escapedSchemaName = schemaName.replaceAll(INVALID_FILE_CHARACTERS_REGEX, "_").trim();
        final String escapedSqlName = databaseName.replaceAll(INVALID_FILE_CHARACTERS_REGEX, "_").trim();
        final String fileName = String
                .format("schema-%s-%s.json", escapedSqlName, escapedSchemaName);

        return schemaFolder.resolve(fileName).toFile();
    }

    @Override
    @SneakyThrows
    public @Nullable DocumentDbSchema read() {
        return read(DEFAULT_SCHEMA_NAME);
    }

    @Override
    @SneakyThrows
    public @Nullable DocumentDbSchema read(@NonNull  final String schemaName) {
        final FileSchemaContainer schemaContainer = getSchemaContainer(schemaName);
        if (schemaContainer == null) {
            return null;
        }
        if (!schemaContainer.getSchema().getSchemaName().equals(schemaName)) {
            return null;
        }
        return schemaContainer.getSchema();
    }


    private @Nullable FileSchemaContainer getSchemaContainer(@NonNull final String schemaName)
            throws IOException {
        final File schemaFile = getSchemaFile(schemaName, databaseName, schemaFolder);
        if (!schemaFile.exists()) {
            return null;
        }
        return OBJECT_MAPPER.readValue(schemaFile, FileSchemaContainer.class);
    }

    @Override
    public @Nullable DocumentDbSchema read(
            @NonNull final String schemaName,
            final int schemaVersion) {
        final DocumentDbSchema schema =  read(schemaName);
        if (schema == null || schema.getSchemaVersion() != schemaVersion) {
            return  null;
        }
        return schema;
    }

    private void verifySchemaVersion(
            final int schemaVersion,
            @NonNull final DocumentDbSchema schema) {
        if (schema.getSchemaVersion() != schemaVersion) {
            throw new IllegalArgumentException(
                    String.format("Given schema version '%s' is not found.", schemaVersion));
        }
    }

    @Override
    @SneakyThrows
    public @NonNull DocumentDbSchemaTable readTable(
            @NonNull final String schemaName,
            final int schemaVersion,
            @NonNull final String tableId) {
        final FileSchemaContainer schemaContainer = getSchemaContainer(schemaName);
        if (schemaContainer == null) {
            throw new IllegalArgumentException(SqlError.lookup(SqlError.MISSING_SCHEMA, schemaName));
        }
        verifySchemaVersion(schemaVersion, schemaContainer.getSchema());
        final Collection<DocumentDbSchemaTable> tableSchemas = schemaContainer.getTableSchemas();
        final DocumentDbSchemaTable tableSchema = tableSchemas.stream()
                .filter(t -> t.getId().equals(tableId)).findFirst().orElse(null);
        if (tableSchema == null) {
            throw new IllegalArgumentException(
                    String.format("Given table ID '%s' is not found.", tableId));
        }
        return tableSchema;
    }

    @Override
    @SneakyThrows
    public Collection<DocumentDbSchemaTable> readTables(
            final String schemaName,
            final int schemaVersion,
            final Set<String> tableIds) {
        final FileSchemaContainer schemaContainer = getSchemaContainer(schemaName);
        if (schemaContainer == null) {
            throw new IllegalArgumentException(SqlError.lookup(SqlError.MISSING_SCHEMA, schemaName));
        }
        verifySchemaVersion(schemaVersion, schemaContainer.getSchema());
        return schemaContainer.getTableSchemas().stream()
                .filter(t -> tableIds.contains(t.getId()))
                .collect(Collectors.toList());
    }
}
