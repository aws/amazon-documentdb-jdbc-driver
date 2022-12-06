/*
 * Copyright <2022> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.documentdb.jdbc.sshtunnel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.util.concurrent.TimeUnit;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.ValidationType.SSH_TUNNEL;

class DocumentDbSshTunnelServerTest {

    @Test
    @Tag("remote-integration")
    void testEnsureStarted() throws Exception {
        final String connectionString = DocumentDbSshTunnelServiceTest.getConnectionString();
        final DocumentDbConnectionProperties properties =
                DocumentDbConnectionProperties.getPropertiesFromConnectionString(connectionString, SSH_TUNNEL);
        DocumentDbSshTunnelServer server = null;
        final int timeoutSECS = 3;
        try  {
            server = DocumentDbSshTunnelServer.builder(
                            properties.getSshUser(),
                            properties.getSshHostname(),
                            properties.getSshPrivateKeyFile(),
                            properties.getHostname())
                    .sshStrictHostKeyChecking(properties.getSshStrictHostKeyChecking())
                    .build();
            server.ensureStarted();
            Assertions.assertTrue(server.getServiceListeningPort() > 0);
            TimeUnit.SECONDS.sleep(timeoutSECS);
            // No clients registered - so should exit
            Assertions.assertFalse(server.isAlive());
        } finally {
            Assertions.assertNotNull(server);
            server.close();
            Assertions.assertEquals(0, server.getServiceListeningPort());
            TimeUnit.SECONDS.sleep(timeoutSECS);
            Assertions.assertFalse(server.isAlive());
        }
    }
}
