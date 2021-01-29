/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 */

package software.amazon.documentdb.jdbc.common.utilities;

/**
 * The interface for connection properties.
 */
public interface ConnectionProperty {
    public static final String APPLICATION_NAME = "appName";

    /**
     * Gets the connection property name.
     *
     * @return the connection property.
     */
    String getName();

    /**
     * Gets the default value of the connection property.
     *
     * @return the default value of the connection property.
     */
    String getDefaultValue();

    /**
     * Gets description.
     *
     * @return the description.
     */
    String getDescription();
}
