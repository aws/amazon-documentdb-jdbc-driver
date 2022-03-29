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

package software.amazon.documentdb.jdbc.common.test;

/**
 * Base class for testing against a DocumentDB server.
 */
public class DocumentDbDocumentDbTest extends DocumentDbTest {
    private static final int DEFAULT_PORT = 27019;
    private static final String DOC_DB_USER_NAME_PROPERTY = "DOC_DB_USER_NAME";
    private static final String DOC_DB_PASSWORD_PROPERTY = "DOC_DB_PASSWORD";
    private static final String DOC_DB_LOCAL_PORT_PROPERTY = "DOC_DB_LOCAL_PORT";
    private static Integer originalMongoPort = -1;

    /**
     * Sets up the environment for remote testing.
     * Call {@link DocumentDbDocumentDbTest#restoreOriginalTesting()} when finished testing.
     */
    public static void setupRemoteTesting() {
        originalMongoPort = getMongoPort();
        setMongoPort(getInteger(System.getenv(DOC_DB_LOCAL_PORT_PROPERTY), DEFAULT_PORT));
    }

    /**
     * Restores the test environment to its previous state.
     */
    public static void restoreOriginalTesting() {
        setMongoPort(originalMongoPort);
    }

    public static String getDocDbUserName() {
        return System.getenv(DOC_DB_USER_NAME_PROPERTY);
    }

    public static String getDocDbPassword() {
        return System.getenv(DOC_DB_PASSWORD_PROPERTY);
    }

    private static Integer getInteger(final String value, final Integer defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
