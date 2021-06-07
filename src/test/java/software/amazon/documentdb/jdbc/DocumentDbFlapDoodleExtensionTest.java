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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import java.io.IOException;

public class DocumentDbFlapDoodleExtensionTest extends DocumentDbFlapDoodleExtension {

    /**
     * Ensures any started instance is stopped.
     */
    @AfterAll
    protected static void cleanup() {
        stopMongoDbInstance();
    }

    /**
     * Tests that mongod can be started and stop using default parameters.
     * @throws IOException if unable to start the process.
     */
    @Test
    protected void testStartMongoDbDefault() throws IOException {
        Assertions.assertFalse(isMongoDbProcessRunning());
        Assertions.assertTrue(startMongoDbInstance());
        Assertions.assertTrue(isMongoDbProcessRunning());
        Assertions.assertTrue(stopMongoDbInstance());
        Assertions.assertFalse(isMongoDbProcessRunning());
    }

    /**
     * Tests that mongod can be started on a non-default port.
     * @throws IOException if unable to start the process.
     */
    @Test
    protected void testStartMongoDbCustomPort() throws IOException {
        Assertions.assertFalse(isMongoDbProcessRunning());
        Assertions.assertTrue(startMongoDbInstance(27018));
        Assertions.assertTrue(isMongoDbProcessRunning());
        Assertions.assertEquals(27018, getMongoPort());
        Assertions.assertTrue(stopMongoDbInstance());
        Assertions.assertFalse(isMongoDbProcessRunning());
    }

    /**
     * Tests that mongod can be started with -auth flag.
     * @throws IOException if unable to start the process.
     */
    @Test
    protected void testStartMongoDbCustomArgs() throws IOException {
        Assertions.assertFalse(isMongoDbProcessRunning());
        Assertions.assertTrue(startMongoDbInstance(true));
        Assertions.assertTrue(isMongoDbProcessRunning());
        Assertions.assertTrue(stopMongoDbInstance());
        Assertions.assertFalse(isMongoDbProcessRunning());
    }

}
