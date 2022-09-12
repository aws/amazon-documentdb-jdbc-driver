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
 * The enumeration of Allow Disk Use options.
 */
public enum DocumentDbAllowDiskUseOption {
    DEFAULT("default"),
    DISABLE("disable"),
    ENABLE("enable"),
    ;

    private final String name;

    DocumentDbAllowDiskUseOption(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns DocumentDbAllowDiskUseOption with a name that matches input string.
     * @param allowDiskUseOption name of the allow disk use option.
     * @return DocumentDbAllowDiskUseOption of string.
     */
    public static DocumentDbAllowDiskUseOption fromString(final String allowDiskUseOption) {
        for (DocumentDbAllowDiskUseOption scanMethod: DocumentDbAllowDiskUseOption.values()) {
            if (scanMethod.name.equals(allowDiskUseOption)) {
                return scanMethod;
            }
        }
        throw new IllegalArgumentException("Invalid allow disk use option.");
    }
}
