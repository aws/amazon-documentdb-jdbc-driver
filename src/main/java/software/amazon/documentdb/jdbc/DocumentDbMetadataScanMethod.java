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
 * The enumeration of methods to scan for metadata.
 */
public enum DocumentDbMetadataScanMethod {
    ID_FORWARD("idForward"),
    ID_REVERSE("idReverse"),
    ALL("all"),
    RANDOM("random");

    private final String name;

    DocumentDbMetadataScanMethod(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns DocumentDbScanMethod with a name that matches input string.
     * @param scanMethodString name of the scan method
     * @return DocumentDbScanMethod of string.
     */
    public static DocumentDbMetadataScanMethod fromString(final String scanMethodString) {
        for (DocumentDbMetadataScanMethod scanMethod: DocumentDbMetadataScanMethod.values()) {
            if (scanMethod.name.equals(scanMethodString)) {
                return scanMethod;
            }
        }
        throw new IllegalArgumentException("Invalid scan method.");
    }
}
