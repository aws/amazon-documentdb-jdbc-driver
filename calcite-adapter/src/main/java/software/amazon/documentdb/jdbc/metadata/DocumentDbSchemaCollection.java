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
import lombok.Getter;
import lombok.NonNull;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;

@Getter
public class DocumentDbSchemaCollection {

    /**
     * The name of the database, same as the DocumentDB database by default.
     */
    @NonNull
    private final String name;

    /**
     * The version number of this metadata.
     */
    private final int version;

    /**
     * The time this metadata was created or updated.
     */
    private final Date modifyDate;

    /**
     * The tables contained within this database.
     */
    @NonNull
    private final ImmutableMap<String, DocumentDbSchemaTable> tables;

    /**
     * All args constructor for collection metadata.
     * @param name Name of the collection.
     * @param version Version of this metadata.
     * @param tables Tables contained within this collection indexed by name. Uses LinkedHashMap to preserve order.
     */
    public DocumentDbSchemaCollection(final String name,
                                      final int version,
                                      final LinkedHashMap<String, DocumentDbSchemaTable> tables) {
        this.name = name;
        this.version = version;
        this.modifyDate = new Date(Instant.now().toEpochMilli());
        this.tables = ImmutableMap.copyOf(tables);
    }

    /**
     * Creates and returns the time the metadata was created or updated.
     *
     * @return a copy of the Date of modification.
     */
    public Date getModifyDate() {
        return new Date(modifyDate.getTime());
    }
}
