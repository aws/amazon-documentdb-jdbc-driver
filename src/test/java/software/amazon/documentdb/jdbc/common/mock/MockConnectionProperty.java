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

package software.amazon.documentdb.jdbc.common.mock;

import software.amazon.documentdb.jdbc.common.utilities.ConnectionProperty;

/**
 * Mocks a ConnectionProperty implementation.
 */
public enum MockConnectionProperty implements ConnectionProperty {
    APPLICATION_NAME(ConnectionProperty.APPLICATION_NAME, "", "Name of the application")
    ;

    private final String connectionProperty;
    private final String defaultValue;
    private final String description;

    MockConnectionProperty(final String connectionProperty, final String defaultValue, final String description) {
        this.connectionProperty = connectionProperty;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    @Override
    public String getName() {
        return connectionProperty;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
