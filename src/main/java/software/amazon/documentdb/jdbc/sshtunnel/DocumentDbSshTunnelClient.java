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

import com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/**
 * The DocumentDbSshTunnelClient class provides a way for connections to ensure
 * a single instance SSH tunnel is started and stays running while this object is alive.
 */
public class DocumentDbSshTunnelClient implements AutoCloseable {
    private final DocumentDbSshTunnelServer sshTunnelServer;
    private final AtomicBoolean closed;
    private final Object lock = new Object();

    /**
     * Creates a new SSH Tunnel client object from the given connection properties.
     *
     * @param properties The connection properties for this SSH Tunnel.
     * @throws SQLException When an error occurs attempting to ensure an SSH Tunnel instance is running.
     */
    public DocumentDbSshTunnelClient(final @NonNull DocumentDbConnectionProperties properties)
            throws SQLException, InterruptedException {
        validateSshTunnelProperties(properties);
        sshTunnelServer = DocumentDbSshTunnelServer.builder(
                        properties.getSshUser(),
                        properties.getSshHostname(),
                        properties.getSshPrivateKeyFile(),
                        properties.getHostname())
                .sshPrivateKeyPassphrase(properties.getSshPrivateKeyPassphrase())
                .sshStrictHostKeyChecking(properties.getSshStrictHostKeyChecking())
                .sshKnownHostsFile(properties.getSshKnownHostsFile())
                .build();
        sshTunnelServer.addClient();
        closed = new AtomicBoolean(false);
    }

    private static void validateSshTunnelProperties(final DocumentDbConnectionProperties properties)
            throws SQLException {
        if (isNullOrWhitespace(properties.getSshUser())
                || isNullOrWhitespace(properties.getSshHostname())
                || isNullOrWhitespace(properties.getSshPrivateKeyFile())
                || isNullOrWhitespace(properties.getHostname())) {
            throw new IllegalArgumentException();
        }
        DocumentDbSshTunnelServer.validateSshPrivateKeyFile(properties);
        DocumentDbSshTunnelServer.getSshKnownHostsFilename(properties);
    }

    /**
     * Gets the SSH tunnel listening port number. If the port number is zero, the SSH Tunnel is not running.
     *
     * @return The SSH tunnel listening port number, or zero.
     */
    public int getServiceListeningPort() {
        return sshTunnelServer.getServiceListeningPort();
    }

    /**
     * Gets indicator of whether the SSH Tunnel server is alive.
     *
     * @return Returns true if the server is alive, false otherwise.
     */
    public boolean isServerAlive() {
        return getSshTunnelServer().isAlive();
    }

    /**
     * Closes the client object by unlocking and deleting the client lock file. If this is the last client
     * for the server, the SSH Tunnel server will be shutdown.
     *
     * @throws Exception When an error occurs unlocking the client lock file or shutting down the server.
     */
    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (closed.get()) {
                return;
            }
            sshTunnelServer.removeClient();
            closed.set(true);
        }
    }

    /**
     * Gets the SSH Tunnel server object.
     *
     * @return An {@link DocumentDbSshTunnelServer} object.
     */
    @VisibleForTesting
    @NonNull DocumentDbSshTunnelServer getSshTunnelServer() {
        return sshTunnelServer;
    }
}
