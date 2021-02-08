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

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Getter;

/**
 * Represents the fields in a collection and their data types. A collection can be broken up into 1
 * (the base table) or more tables (virtual tables from embedded documents or arrays).
 */
@Builder
@Getter
public class DocumentDbCollectionMetadata {

    /** The name of the collection in the database. */
    private final String path;

    /** The tables the collection is composed of indexed by their path. */
    private final ImmutableMap<String, DocumentDbMetadataTable> tables;
}
