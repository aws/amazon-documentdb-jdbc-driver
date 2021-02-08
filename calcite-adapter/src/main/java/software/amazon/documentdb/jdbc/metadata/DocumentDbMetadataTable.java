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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/** Represents the fields in a document, embedded document or array. */
@Builder
@Getter
public class DocumentDbMetadataTable {

    /** The original path to the collection (if the base table) or field (if a virtual table). */
    private final String path;

    /** The path to the parent table. Null if the table is the base table. */
    private final String parentPath;

    /** The paths to any children tables. **/
    private final ImmutableList<String> childPaths;

    /** The columns the table is composed of indexed by their path. */
    private final ImmutableMap<String, DocumentDbMetadataColumn> columns;

    /** The display name of the table. */
    @Setter private String name;
}
