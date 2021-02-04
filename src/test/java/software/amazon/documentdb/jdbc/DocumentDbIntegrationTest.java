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

package software.amazon.documentdb.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.util.regex.Pattern;

/**
 * DocumentDB integration tests
 */
public class DocumentDbIntegrationTest {

    /**
     * Ensure local environment is correctly setup
     */
    @Test
    @Tag("local-integration")
    public void runLocalTest() {
        Assertions.assertEquals(true, true);
        final String connectionString = System.getenv("connectionString");
        Assertions.assertNotNull(connectionString);
        Assertions.assertTrue(Pattern.matches(".*localhost.*", connectionString));
    }

    /**
     * Ensure remote environment is correctly setup
     */
    @Test
    @Tag("remote-integration")
    public void runRemoteTest() {
        Assertions.assertEquals(true, true);
        final String connectionString = System.getenv("connectionString");
        Assertions.assertNotNull(connectionString);
        Assertions.assertTrue(Pattern.matches(".*remotehost.*", connectionString));
    }
}
