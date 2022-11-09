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

package software.amazon.documentdb.jdbc.sshtunnel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.ValidationType.SSH_TUNNEL;

class DocumentDbSshTunnelClientTest {

    @Test
    @Tag("remote-integration")
    void testConstructorDestructor() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();

        DocumentDbSshTunnelClient client = null;
        DocumentDbSshTunnelServer server = null;
        try {
            client = new DocumentDbSshTunnelClient(properties);
            server = client.getSshTunnelServer();
            Assertions.assertTrue(client.getServiceListeningPort() > 0);
            TimeUnit.SECONDS.sleep(1);
            Assertions.assertTrue(client.isServerAlive());
            TimeUnit.SECONDS.sleep(1);
            Assertions.assertTrue(client.isServerAlive());
        } finally {
            Assertions.assertNotNull(client);
            client.close();
            // This is the only client, so server will shut down.
            TimeUnit.SECONDS.sleep(1);
            Assertions.assertNotNull(server);
            Assertions.assertFalse(client.isServerAlive());
        }
    }

    @Test
    void testInvalidConnectionProperties() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshUser("");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new DocumentDbSshTunnelClient(properties));
    }

    @Test
    void testMultipleClientsSameServer() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        final List<DocumentDbSshTunnelClient> clients = new ArrayList<>();

        try {
            for (int i = 0; i < 50; i++) {
                final DocumentDbSshTunnelClient client = new DocumentDbSshTunnelClient(properties);
                Assertions.assertNotNull(client);
                for (DocumentDbSshTunnelClient compareClient : clients) {
                    // Each client is different
                    Assertions.assertNotEquals(client, compareClient);
                    // Each server with the same connection properties has the same server
                    Assertions.assertEquals(client.getSshTunnelServer(), compareClient.getSshTunnelServer());
                }
                clients.add(client);
            }
        } finally {
            for (DocumentDbSshTunnelClient client : clients) {
                client.close();
            }
        }
    }

    @Test
    void testInvalidSshHostnameUnreachable() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshHostname("254.254.254.254");

        final Exception e = Assertions.assertThrows(
                SQLException.class,
                () -> new DocumentDbSshTunnelClient(properties));
        Assertions.assertEquals("java.sql.SQLException: Error reported from SSH Tunnel service."
                        + " (Server exception detected: 'java.sql.SQLException: java.net.SocketException:"
                        + " Network is unreachable: connect')",
                e.toString());
    }

    @Test
    void testInvalidSshHostnameConnectionTimeout() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshHostname("10.1.1.1");

        final Exception e = Assertions.assertThrows(
                SQLException.class,
                () -> new DocumentDbSshTunnelClient(properties));
        Assertions.assertEquals("java.sql.SQLException: Error reported from SSH Tunnel service."
                        + " (Server exception detected: 'java.sql.SQLException: java.net.ConnectException:"
                        + " Connection timed out: connect')",
                e.toString());
    }

    @Test
    void testInvalidSshUserAuthFail() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshUser("unknown");

        final Exception e = Assertions.assertThrows(
                SQLException.class,
                () -> new DocumentDbSshTunnelClient(properties));
        Assertions.assertEquals("java.sql.SQLException: Error reported from SSH Tunnel service."
                        + " (Server exception detected: 'java.sql.SQLException: Auth fail')",
                e.toString());
    }

    @Test
    void testInvalidSshPrivateKeyFileNotFound() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshPrivateKeyFile("unknown");

        final Exception e = Assertions.assertThrows(
                SQLException.class,
                () -> new DocumentDbSshTunnelClient(properties));
        Assertions.assertEquals("java.sql.SQLException: SSH private key file 'unknown' not found.",
                e.toString());
    }

    @Test
    void testInvalidSshKnownHostsFileNotFound() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshStrictHostKeyChecking("true");
        properties.setSshKnownHostsFile("unknown");

        final Exception e = Assertions.assertThrows(
                SQLException.class,
                () -> new DocumentDbSshTunnelClient(properties));
        Assertions.assertEquals("java.sql.SQLException: 'Known hosts' file 'unknown' not found.",
                e.toString());
    }

    @Test
    void testConnectionsInMultipleProcesses() {
    }

    private static DocumentDbConnectionProperties getConnectionProperties() throws SQLException {
        final String connectionString = DocumentDbSshTunnelServiceTest.getConnectionString();
        return DocumentDbConnectionProperties.getPropertiesFromConnectionString(connectionString, SSH_TUNNEL);
    }
}
