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

package software.amazon.documentdb.jdbc;

/**
 * The schema storage type.
 */
public enum DocumentDbSchemaStoreType {
    FILE("file"),
    DATABASE("database");

    private final String name;

    DocumentDbSchemaStoreType(final String name) {
        this.name = name;
    }

    /**
     * Gets the name of the enumerated value.
     *
     * @return the name of the enumerated value.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns {@link DocumentDbSchemaStoreType} with a name that matches input string.
     *
     * @param storeTypeString name of the scan method
     * @return a {@link DocumentDbSchemaStoreType} that matches the string.
     */
    public static DocumentDbSchemaStoreType fromString(final String storeTypeString) {
        for (DocumentDbSchemaStoreType storeType: DocumentDbSchemaStoreType.values()) {
            if (storeType.name.equals(storeTypeString)) {
                return storeType;
            }
        }
        throw new IllegalArgumentException("Invalid scan method.");
    }
}
