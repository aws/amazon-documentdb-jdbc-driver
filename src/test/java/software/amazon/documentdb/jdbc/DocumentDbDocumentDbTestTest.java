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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.common.test.DocumentDbDocumentDbTest;

public class DocumentDbDocumentDbTestTest extends DocumentDbDocumentDbTest {
    private static final String AUTH_DATABASE = "admin";

    @BeforeAll
    static void initialize() {
        setupRemoteTesting();
    }

    @AfterAll
    static void cleanup() {
        restoreOriginalTesting();
    }

    @Disabled("Doesn't support tlsCAFile option.")
    @Tag("remote-integration")
    @Test
    void testConnectivity() {
        Assertions.assertNotNull(getDocDbUserName());
        Assertions.assertNotNull(getDocDbPassword());

        try (MongoClient client = createMongoClient(AUTH_DATABASE, getDocDbUserName(), getDocDbPassword(),
                "?tls=true&tlsAllowInvalidHostnames=true")) {
            final MongoDatabase database = client.getDatabase("integration");
            final Document result = database.runCommand(new Document("ping", 1));
            Assertions.assertTrue(result.containsKey("ok"));
            Assertions.assertEquals(1.0, result.getDouble("ok"));
            Assertions.assertTrue(result.containsKey("operationTime"));
        }
    }
}
