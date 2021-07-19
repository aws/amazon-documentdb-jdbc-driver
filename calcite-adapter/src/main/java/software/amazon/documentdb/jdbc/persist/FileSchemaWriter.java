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

import lombok.NonNull;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaException;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.USER_HOME_PROPERTY;
import static software.amazon.documentdb.jdbc.persist.FileSchemaReader.DEFAULT_FOLDER;
import static software.amazon.documentdb.jdbc.persist.FileSchemaReader.OBJECT_MAPPER;
import static software.amazon.documentdb.jdbc.persist.FileSchemaReader.getSchemaFile;

public class FileSchemaWriter implements SchemaWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSchemaWriter.class);
    private final Path schemaFolder;
    private final String sqlName;

    /**
     * Creates a new {@link FileSchemaReader} from a given SQL database name using the default
     * path of {@code ~/.documentdb/}.
     *
     * @param sqlName the SQL database name.
     */
    public FileSchemaWriter(final String sqlName) {
        this(sqlName, Paths.get(System.getProperty(USER_HOME_PROPERTY), DEFAULT_FOLDER));
    }

    /**
     * Creates a new {@link FileSchemaWriter} from the given SQL database name and file system
     * folder path.
     *
     * @param sqlName the SQL database name.
     * @param schemaFolder the {@link Path} to the folder where schema is stored.
     */
    public FileSchemaWriter(
            @NonNull final String sqlName,
            @NonNull final Path schemaFolder) {
        this.sqlName = sqlName;
        this.schemaFolder = schemaFolder;
    }

    @Override
    @SneakyThrows
    public void write(
            @NonNull final DocumentDbSchema schema,
            @NonNull final Collection<DocumentDbSchemaTable> tableSchemas) {
        final File schemaFile = getSchemaFile(schema.getSchemaName(), sqlName, schemaFolder);
        final FileSchemaContainer container = new FileSchemaContainer(schema, tableSchemas);
        OBJECT_MAPPER.writeValue(schemaFile, container);
    }

    @Override
    public void update(
            @NonNull final DocumentDbSchema schema,
            final Collection<DocumentDbSchemaTable> tableSchemas) throws SQLException {
        final String schemaName = schema.getSchemaName();
        final File schemaFile = getSchemaFile(schemaName, sqlName, schemaFolder);
        final FileSchemaContainer schemaContainer;
        if (schemaFile.exists()) {
            try {
                schemaContainer = OBJECT_MAPPER
                        .readValue(schemaFile, FileSchemaContainer.class);
                if (!schemaContainer.getSchema().getSchemaName().equals(schemaName)) {
                    throw new IllegalArgumentException(
                            SqlError.lookup(SqlError.MISMATCH_SCHEMA_NAME,
                                    schemaName,
                                    schemaContainer.getSchema().getSchemaName()));
                }
            } catch (IOException e) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.DATA_EXCEPTION,
                        e,
                        SqlError.MISMATCH_SCHEMA_NAME);
            }
        } else {
            schemaContainer = new FileSchemaContainer(schema, tableSchemas);
        }

        // Restore the matching tables.
        final Map<String, DocumentDbSchemaTable> newTableMap = schemaContainer.getTableSchemas().stream()
                .filter(t -> schemaContainer.getSchema().getTableReferences().contains(t.getId()))
                .collect(Collectors.toMap(DocumentDbSchemaTable::getSqlName, t -> t, (t1, t2) -> t1));
        newTableMap.putAll(tableSchemas.stream()
                .collect(Collectors.toMap(DocumentDbSchemaTable::getSqlName, t -> t)));
        final DocumentDbSchema newSchema = new DocumentDbSchema(
                schema.getSchemaName(),
                schema.getSqlName(),
                schemaContainer.getSchema().getSchemaVersion() + 1,
                newTableMap);

        write(newSchema, newTableMap.values());
    }

    @Override
    @SneakyThrows
    public void remove(final String schemaName) {
        final File schemaFile = getSchemaFile(schemaName, sqlName, schemaFolder);
        if (schemaFile.exists()) {
            if (!schemaFile.delete()) {
                throw new DocumentDbSchemaException("Unable to delete schema file.");
            }
        }
    }

    @Override
    @SneakyThrows
    public void remove(final String schemaName, final int schemaVersion) {
        final File schemaFile = getSchemaFile(schemaName, sqlName, schemaFolder);
        if (schemaFile.exists()) {
            final FileSchemaContainer schemaContainer = OBJECT_MAPPER
                    .readValue(schemaFile, FileSchemaContainer.class);
            if (schemaContainer == null
                    || schemaContainer.getSchema() == null
                    || schemaContainer.getSchema().getSchemaVersion() == schemaVersion) {
                if (!schemaFile.delete()) {
                    throw new DocumentDbSchemaException("Unable to delete schema file.");
                }
            }
        }
    }
}
