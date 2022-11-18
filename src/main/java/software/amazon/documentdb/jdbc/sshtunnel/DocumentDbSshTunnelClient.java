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

import com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.UUID;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/**
 * The DocumentDbSshTunnelClient class provides a way for connections to ensure
 * a single instance SSH tunnel is started and stays running while this object is alive.
 */
public class DocumentDbSshTunnelClient implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSshTunnelClient.class);
    private final Object mutex = new Object();

    private final String propertiesHashString;
    private final DocumentDbSshTunnelServer sshTunnelServer;

    private volatile FileLock clientLock = null;
    private volatile DocumentDbMultiThreadFileChannel clientChannel = null;
    private volatile Path clientLockPath = null;

    /**
     * Creates a new SSH Tunnel client object from the given connection properties.
     *
     * @param properties The connection properties for this SSH Tunnel.
     * @throws Exception When an error occurs attempting to ensure an SSH Tunnel instance is running.
     */
    public DocumentDbSshTunnelClient(final @NonNull DocumentDbConnectionProperties properties)
            throws Exception {
        validateSshTunnelProperties(properties);
        this.propertiesHashString = DocumentDbSshTunnelLock.getHashString(
                properties.getSshUser(),
                properties.getSshHostname(),
                properties.getSshPrivateKeyFile(),
                properties.getHostname());

        try {
            ensureClientLocked();
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
        } catch (Exception e) {
            ensureClientUnlocked();
            throw e;
        }
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
     * @throws Exception When an error occurs trying to determine if the server is alive.
     */
    public boolean isServerAlive() throws Exception {
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
        synchronized (mutex) {
            ensureClientUnlocked();
            sshTunnelServer.removeClient();
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

    /**
     * Ensures the client lock file is created and locked.
     *
     * @throws Exception When an error occurs trying to create and lock the client lock file.
     */
    private void ensureClientLocked() throws Exception {
        initializeClientLockFolder();
        final Exception exception = DocumentDbSshTunnelLock.runInGlobalLock(propertiesHashString, this::lockClientFile);
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Initializes the client by ensuring the parent folder exists, gets a UUID for this client lock file.
     *
     * @throws IOException When an error occurs trying to create the parent directories.
     */
    private void initializeClientLockFolder() throws IOException {
        final UUID unique = UUID.randomUUID();
        clientLockPath = DocumentDbSshTunnelLock.getClientLockPath(unique, propertiesHashString);
        final Path parentPath = clientLockPath.getParent();
        assert parentPath != null;
        Files.createDirectories(parentPath);
    }

    /**
     * Locks the client lock file. Assumes it is run inside the global lock context.
     *
     * @return An Exception if an error occurs locking the client lock file, null otherwise.
     */
    private Exception lockClientFile() {
        Exception e = null;
        try {
            clientChannel = DocumentDbMultiThreadFileChannel.open(
                    clientLockPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            clientLock = clientChannel.lock();
            LOGGER.info("SSH Tunnel server client lock active.");
        } catch (Exception ex) {
            e = ex;
        }
        return e;
    }

    /**
     * Ensures the client is unlocked. Is run in the global lock context.
     *
     * @throws Exception When an error occurs unlocking the client lock file.
     */
    private void ensureClientUnlocked() throws Exception {
        final Exception exception = DocumentDbSshTunnelLock.runInGlobalLock(
                propertiesHashString, this::unlockClientFile);
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Unlocks the client lock file and cleans up the file from the folder.
     *
     * @return An exception if an error occurs unlocking the click lock file, null otherwise.
     */
    private Exception unlockClientFile() {
        Exception exception = null;
        try {
            if (clientLock != null && clientLock.isValid()) {
                clientLock.close();
                LOGGER.info("SSH Tunnel server client lock inactive.");
            }
            if (clientChannel != null && clientChannel.isOpen()) {
                clientChannel.close();
            }
            if (clientLockPath != null) {
                Files.deleteIfExists(clientLockPath);
            }
        } catch (Exception e) {
            exception = e;
        }
        clientLock = null;
        clientChannel = null;
        clientLockPath = null;
        return exception;
    }
}
