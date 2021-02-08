/*
 * Copyright <2020> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc.metadata;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/** Represents a field in a document, embedded document or array as a column in a table. */
@Builder
@Getter
public class DocumentDbMetadataColumn {

    /** Original path to the field in the collection. */
    private final String path;

    /**
     * {@code true} if the column was generated rather than taken directly from the collection;
     * {@code false} otherwise.
     */
    private final boolean isGenerated;

    /**
     * Indicates the position of the column in the primary key of the table; 0 if not part of the key.
     */
    private final int primaryKey;

    /**
     * Indicates the position of the column in the foreign key of the table; 0 if not part of the key.
     */
    private final int foreignKey;

    /** Display name of the field. */
    @Setter private String name;

    /** SQL/JDBC type of the field. Refer to the types in {@link java.sql.Types} */
    @Setter private int sqlType;
}
