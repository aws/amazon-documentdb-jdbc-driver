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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;

import java.util.ArrayList;
import java.util.Collection;

@Getter
class FileSchemaContainer {

    public static final String SCHEMA_PROPERTY = "schema";
    public static final String TABLE_SCHEMAS_PROPERTY = "tableSchemas";
    /**
     * The database schema.
     */
    private final DocumentDbSchema schema;

    /**
     * The table schemas collection.
     */
    private final Collection<DocumentDbSchemaTable> tableSchemas;

    @JsonCreator
    public FileSchemaContainer(
            @JsonProperty(SCHEMA_PROPERTY)
            final @NonNull DocumentDbSchema schema,
            @JsonProperty(TABLE_SCHEMAS_PROPERTY)
            final Collection<DocumentDbSchemaTable> tableSchemas) {
        this.schema = schema;
        this.tableSchemas = tableSchemas != null ? tableSchemas : new ArrayList<>();
    }
}
