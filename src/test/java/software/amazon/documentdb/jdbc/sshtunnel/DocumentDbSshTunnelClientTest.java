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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.ValidationType.SSH_TUNNEL;

class DocumentDbSshTunnelClientTest {
    private static final String DOC_DB_PRIV_KEY_FILE_PROPERTY = "DOC_DB_PRIV_KEY_FILE";
    private static final String DOC_DB_USER_PROPERTY = "DOC_DB_USER";
    private static final String DOC_DB_HOST_PROPERTY = "DOC_DB_HOST";

    @Test
    @Tag("remote-integration")
    void testConstructorDestructor() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();

        DocumentDbSshTunnelClient client = null;
        DocumentDbSshTunnelServer server = null;
        try {
            client = new DocumentDbSshTunnelClient(properties);
            server = client.getSshTunnelServer();
            server.setCloseDelayMS(1000);
            Assertions.assertTrue(client.getServiceListeningPort() > 0);
            TimeUnit.SECONDS.sleep(1);
            Assertions.assertTrue(client.isServerAlive());
            TimeUnit.SECONDS.sleep(1);
            Assertions.assertTrue(client.isServerAlive());
        } finally {
            if (client != null) {
                client.close();
                // This is the only client, so server will shut down.
                TimeUnit.MILLISECONDS.sleep(server.getCloseDelayMS() + 500);
                Assertions.assertNotNull(server);
                Assertions.assertFalse(client.isServerAlive());
            }
        }
    }

    @Test
    @Tag("remote-integration")
    void testInvalidConnectionProperties() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshUser("");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new DocumentDbSshTunnelClient(properties));
    }

    @Test
    @Tag("remote-integration")
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
            int clientCount = clients.size();
            final DocumentDbSshTunnelServer server = clients.get(0).getSshTunnelServer();
            server.setCloseDelayMS(0);
            for (DocumentDbSshTunnelClient client : clients) {
                client.close();
                clientCount--;
                if (clientCount > 0) {
                    Assertions.assertTrue(client.getSshTunnelServer().isAlive());
                } else {
                    Assertions.assertFalse(client.getSshTunnelServer().isAlive());
                }
            }
        }
    }

    @Test
    @Tag("remote-integration")
    void testInvalidSshHostnameConnectionTimeout() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshHostname("2.2.2.2");

        final Exception e = Assertions.assertThrows(
                SQLException.class,
                () -> new DocumentDbSshTunnelClient(properties));
        Assertions.assertTrue(e.toString().startsWith(
                "java.sql.SQLException: java.net.ConnectException: Connection timed out"));
    }

    @Test
    @Tag("remote-integration")
    void testInvalidSshUserAuthFail() throws Exception {
        final DocumentDbConnectionProperties properties = getConnectionProperties();
        properties.setSshUser("unknown");

        final Exception e = Assertions.assertThrows(
                SQLException.class,
                () -> new DocumentDbSshTunnelClient(properties));
        Assertions.assertEquals("java.sql.SQLException: Auth fail for methods 'publickey,gssapi-keyex,gssapi-with-mic'",
                e.toString());
    }

    @Test
    @Tag("remote-integration")
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
    @Tag("remote-integration")
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

    private static DocumentDbConnectionProperties getConnectionProperties() throws SQLException {
        final String connectionString = getConnectionString();
        return DocumentDbConnectionProperties.getPropertiesFromConnectionString(connectionString, SSH_TUNNEL);
    }

    static String getConnectionString() {
        final String docDbRemoteHost = System.getenv(DOC_DB_HOST_PROPERTY);
        final String docDbSshUserAndHost = System.getenv(DOC_DB_USER_PROPERTY);
        final int userSeparatorIndex = docDbSshUserAndHost.indexOf('@');
        final String sshUser = docDbSshUserAndHost.substring(0, userSeparatorIndex);
        final String sshHostname = docDbSshUserAndHost.substring(userSeparatorIndex + 1);
        final String docDbSshPrivKeyFile = System.getenv(DOC_DB_PRIV_KEY_FILE_PROPERTY);
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
        properties.setHostname(docDbRemoteHost);
        properties.setSshUser(sshUser);
        properties.setSshHostname(sshHostname);
        properties.setSshPrivateKeyFile(docDbSshPrivKeyFile);
        properties.setSshStrictHostKeyChecking(String.valueOf(false));
        return DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME + properties.buildSshConnectionString();
    }
}
