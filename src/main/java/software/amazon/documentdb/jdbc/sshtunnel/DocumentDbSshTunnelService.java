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

import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.ValidationType.SSH_TUNNEL;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getPath;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/**
 * The DocumentDbSshTunnelService class provide a runnable service to host an SSH Tunnel.
 * It monitors the running clients and exits when there are no more active clients.
 */
public class DocumentDbSshTunnelService implements AutoCloseable, Runnable {
    public static final String SSH_KNOWN_HOSTS_FILE = "~/.ssh/known_hosts";
    public static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    public static final String HASH_KNOWN_HOSTS = "HashKnownHosts";
    public static final String SERVER_HOST_KEY = "server_host_key";
    public static final String YES = "yes";
    public static final String NO = "no";
    public static final String LOCALHOST = "localhost";
    public static final int DEFAULT_DOCUMENTDB_PORT = 27017;
    public static final int DEFAULT_SSH_PORT = 22;
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSshTunnelService.class);
    public static final int CLIENT_WATCH_POLL_TIME = 500;
    private final DocumentDbConnectionProperties connectionProperties;
    private final String sshPropertiesHashString;
    private volatile boolean completed = false;
    private volatile boolean interrupted = false;
    private final ConcurrentLinkedDeque<Exception> exceptions = new ConcurrentLinkedDeque<>();

    /**
     * Constructs a new instance of DocumentDbSshTunnelService.
     *
     * @param connectionString the SSH tunnel connection string properties.
     * @throws SQLException thrown if unable to parse connection string.
     */
    public DocumentDbSshTunnelService(final String connectionString) throws SQLException {
        connectionProperties = DocumentDbConnectionProperties.getPropertiesFromConnectionString(
                connectionString, SSH_TUNNEL);
        sshPropertiesHashString = DocumentDbSshTunnelLock.getHashString(
                connectionProperties.getSshUser(),
                connectionProperties.getSshHostname(),
                connectionProperties.getSshPrivateKeyFile(),
                connectionProperties.getHostname());
    }

    @Override
    @SneakyThrows
    public void close() {
        interrupted = true;
    }

    /**
     * Runs the SSH tunnel and polls for client lock files.
     * When all the client lock files are gone or unlocked, then this method stops and
     * cleans up any resources.
     */
    @Override
    public void run() {
        SshPortForwardingSession session = null;
        DocumentDbMultiThreadFileChannel serverChannel = null;
        FileLock serverLock = null;

        while (!interrupted && !completed) {
            try {
                LOGGER.debug("SSH Tunnel service starting.");
                session = performSshTunnelSessionStartup();
                final Map.Entry<DocumentDbMultiThreadFileChannel , FileLock> lock = acquireServerLock();
                serverChannel = lock.getKey();
                serverLock = lock.getValue();
                LOGGER.debug("SSH Tunnel service started.");

                // launch thread and wait for clients to terminate.
                waitForClients(serverLock);
            } catch (InterruptedException e) {
                logException(e);
                interrupted = true;
            } catch (Exception e) {
                exceptions.add(logException(e));
            } finally {
                try {
                    LOGGER.debug("SSH Tunnel service stopping.");
                    cleanupResourcesInGlobalLock(session, serverChannel, serverLock);
                } catch (Exception e) {
                    exceptions.add(logException(e));
                }
                completed = true;
            }
        }
        LOGGER.debug("SSH Tunnel service stopped.");
    }

    private void cleanupResourcesInGlobalLock(
            final SshPortForwardingSession session,
            final DocumentDbMultiThreadFileChannel serverChannel,
            final FileLock serverLock) throws Exception {
        DocumentDbSshTunnelLock.runInGlobalLock(
                sshPropertiesHashString,
                () -> closeResources(session, serverChannel, serverLock, exceptions));
    }

    private Queue<Exception> closeResources(
            final SshPortForwardingSession session,
            final DocumentDbMultiThreadFileChannel serverChannel,
            final FileLock serverLock,
            final Queue<Exception> exceptions) {
        try {
            if (serverLock != null && serverLock.isValid()) {
                serverLock.close();
            }
            if (serverChannel != null && serverChannel.isOpen()) {
                serverChannel.close();
            }
            if (session != null) {
                session.getSession().disconnect();
            }
            final Path portLockPath = DocumentDbSshTunnelLock.getPortLockPath(sshPropertiesHashString);
            final Path startupLockPath = DocumentDbSshTunnelLock.getStartupLockPath(sshPropertiesHashString);
            Files.deleteIfExists(portLockPath);
            Files.deleteIfExists(startupLockPath);
        } catch (Exception e) {
            exceptions.add(logException(e));
        }
        return exceptions;
    }

    /**
     * Gets the SSH tunnel properties hash string.
     *
     * @return a {@link  String} representing the SSH tunnel properties hash string.
     */
    public String getSshPropertiesHashString() {
        return sshPropertiesHashString;
    }

    private void
    waitForClients(final FileLock serverLock) throws InterruptedException {
        ClientWatcher clientWatcher = null;
        try {
            clientWatcher = new ClientWatcher(serverLock, sshPropertiesHashString);
            final Thread clientWatcherThread = new Thread(clientWatcher);
            clientWatcherThread.setDaemon(true);
            clientWatcherThread.start();
            do {
                clientWatcherThread.join(1000);
            } while (clientWatcherThread.isAlive() && !interrupted);
        } finally {
            if (clientWatcher != null) {
                exceptions.addAll(clientWatcher.getExceptions());
            }
        }
    }

    /**
     * Closes (and unlocks) the server lock if not already unlocked.
     */
    private static Exception closeServerLock(final FileLock serverLock) {
        Exception result = null;
        if (serverLock != null && serverLock.isValid()) {
            try {
                serverLock.close();
            } catch (IOException e) {
                result = logException(e);
            }
        }
        return result;
    }


    /**
     * Initializes the SSH session and creates a port forwarding tunnel.
     *
     * @param connectionProperties the {@link DocumentDbConnectionProperties} connection properties.
     * @return a {@link Session} session. This session must be closed by calling the
     *          {@link Session#disconnect()} method.
     * @throws SQLException if unable to create SSH session or create the port forwarding tunnel.
     */
    public static SshPortForwardingSession createSshTunnel(
            final DocumentDbConnectionProperties connectionProperties) throws SQLException {
        DocumentDbSshTunnelServer.validateSshPrivateKeyFile(connectionProperties);

        LOGGER.debug("Internal SSH tunnel starting.");
        try {
            final JSch jSch = new JSch();
            addIdentity(connectionProperties, jSch);
            final Session session = createSession(connectionProperties, jSch);
            connectSession(connectionProperties, jSch, session);
            final SshPortForwardingSession portForwardingSession = getPortForwardingSession(
                    connectionProperties, session);
            LOGGER.debug("Internal SSH tunnel started on local port '{}'.",
                    portForwardingSession.getLocalPort());
            return portForwardingSession;
        } catch (Exception e) {
            throw logException(e);
        }
    }

    private static SshPortForwardingSession getPortForwardingSession(
            final DocumentDbConnectionProperties connectionProperties,
            final Session session) throws JSchException {
        final Pair<String, Integer> clusterHostAndPort = getHostAndPort(
                connectionProperties.getHostname(), DEFAULT_DOCUMENTDB_PORT);
        final int localPort = session.setPortForwardingL(
                LOCALHOST, 0, clusterHostAndPort.getLeft(), clusterHostAndPort.getRight());
        return new SshPortForwardingSession(session, localPort);
    }

    private static Pair<String, Integer> getHostAndPort(
            final String hostname,
            final int defaultPort) {
        final String clusterHost;
        final int clusterPort;
        final int portSeparatorIndex = hostname.indexOf(':');
        if (portSeparatorIndex >= 0) {
            clusterHost = hostname.substring(0, portSeparatorIndex);
            clusterPort = Integer.parseInt(
                    hostname.substring(portSeparatorIndex + 1));
        } else {
            clusterHost = hostname;
            clusterPort = defaultPort;
        }
        return new ImmutablePair<>(clusterHost, clusterPort);
    }

    private static void connectSession(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch,
            final Session session) throws SQLException {
        setSecurityConfig(connectionProperties, jSch, session);
        try {
            session.connect();
        } catch (JSchException e) {
            throw logException(e);
        }
    }

    private static void addIdentity(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch) throws JSchException {
        final String privateKeyFileName = getPath(connectionProperties.getSshPrivateKeyFile(),
                DocumentDbConnectionProperties.getDocumentDbSearchPaths()).toString();
        LOGGER.debug("SSH private key file resolved to '{}'.", privateKeyFileName);
        // If passPhrase protected, will need to provide this, too.
        final String passPhrase = !isNullOrWhitespace(connectionProperties.getSshPrivateKeyPassphrase())
                ? connectionProperties.getSshPrivateKeyPassphrase()
                : null;
        jSch.addIdentity(privateKeyFileName, passPhrase);
    }

    private static Session createSession(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch) throws SQLException {
        final String sshUsername = connectionProperties.getSshUser();
        final Pair<String, Integer> sshHostAndPort = getHostAndPort(
                connectionProperties.getSshHostname(), DEFAULT_SSH_PORT);
        setKnownHostsFile(connectionProperties, jSch);
        try {
            return jSch.getSession(sshUsername, sshHostAndPort.getLeft(), sshHostAndPort.getRight());
        } catch (JSchException e) {
            throw logException(e);
        }
    }

    private static void setSecurityConfig(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch,
            final Session session) {
        if (!connectionProperties.getSshStrictHostKeyChecking()) {
            session.setConfig(STRICT_HOST_KEY_CHECKING, NO);
            return;
        }
        setHostKeyType(connectionProperties, jSch, session);
    }

    private static void setHostKeyType(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch, final Session session) {
        final HostKeyRepository keyRepository = jSch.getHostKeyRepository();
        final HostKey[] hostKeys = keyRepository.getHostKey();
        final Pair<String, Integer> sshHostAndPort = getHostAndPort(
                connectionProperties.getSshHostname(), DEFAULT_SSH_PORT);
        final HostKey hostKey = Arrays.stream(hostKeys)
                .filter(hk -> hk.getHost().equals(sshHostAndPort.getLeft()))
                .findFirst().orElse(null);
        // This will ensure a match between how the host key was hashed in the known_hosts file.
        final String hostKeyType = (hostKey != null) ? hostKey.getType() : null;
        // Append the hash algorithm
        if (hostKeyType != null) {
            session.setConfig(SERVER_HOST_KEY, session.getConfig(SERVER_HOST_KEY) + "," + hostKeyType);
        }
        // The default behaviour of `ssh-keygen` is to hash known hosts keys
        session.setConfig(HASH_KNOWN_HOSTS, YES);
    }

    private static void setKnownHostsFile(
            final DocumentDbConnectionProperties connectionProperties,
            final JSch jSch) throws SQLException {
        if (!connectionProperties.getSshStrictHostKeyChecking()) {
            return;
        }
        final String knowHostsFilename;
        knowHostsFilename = DocumentDbSshTunnelServer.getSshKnownHostsFilename(connectionProperties);
        try {
            jSch.setKnownHosts(knowHostsFilename);
        } catch (JSchException e) {
            throw logException(e);
        }
    }
    private Map.Entry<DocumentDbMultiThreadFileChannel, FileLock> acquireServerLock() throws IOException, InterruptedException {
        final Path serverLockPath = DocumentDbSshTunnelLock.getServerLockPath(sshPropertiesHashString);
        final Path parentPath = serverLockPath.getParent();
        assert parentPath != null;
        Files.createDirectories(parentPath);
        final DocumentDbMultiThreadFileChannel serverChannel = DocumentDbMultiThreadFileChannel.open(
                serverLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock serverLock;
        final int pollTimeMS = 100;
        while ((serverLock = serverChannel.tryLock()) == null) {
            TimeUnit.MILLISECONDS.sleep(pollTimeMS);
        }
        return new AbstractMap.SimpleImmutableEntry<>(serverChannel, serverLock);
    }

    private SshPortForwardingSession performSshTunnelSessionStartup()
            throws Exception {
        if (!connectionProperties.enableSshTunnel()) {
            throw new UnsupportedOperationException(
                    "Unable to create SSH tunnel session. Invalid properties provided.");
        }
        final SshPortForwardingSession session;
        final Path startupLockPath = DocumentDbSshTunnelLock.getStartupLockPath(sshPropertiesHashString);
        final Path parentPath = startupLockPath.getParent();
        assert parentPath != null;
        Files.createDirectories(parentPath);
        try (DocumentDbMultiThreadFileChannel startupChannel = DocumentDbMultiThreadFileChannel.open(
                startupLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                     Channels.newOutputStream(startupChannel.getFileChannel()),
                     StandardCharsets.UTF_8));
             FileLock ignored = startupChannel.lock()) {
            try {
                session = createSshTunnel(connectionProperties);
            } catch (Exception e) {
                logException(e);
                writer.write(e.toString());
                throw e;
            }
            writeSssTunnelPort(session);
        }
        return session;
    }

    private void writeSssTunnelPort(final SshPortForwardingSession session) throws IOException {
        final Path portLockPath = DocumentDbSshTunnelLock.getPortLockPath(sshPropertiesHashString);
        try (FileOutputStream outputStream = new FileOutputStream(portLockPath.toFile());
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
             FileLock ignored = outputStream.getChannel().lock()) {
            writer.write(String.format("%d%n", session.getLocalPort()));
        }
    }

    /**
     * Gets a copy of the list of exceptions raised while the service is running.
     *
     * @return a list of exceptions raised while the service is running.
     */
    public List<Exception> getExceptions() {
        return Collections.unmodifiableList(new ArrayList<>(exceptions));
    }

    /**
     * The ClientWatcher class implements the {@link Runnable} interface. When run,
     * it monitors the clients lock folder for client lock files. When no more locked
     * files exist, the run method ends. Before exiting, it will release the passed server lock file
     * inside the global lock to ensure that there will not be a race condition with new clients trying
     * to start up.
     */
    private static class ClientWatcher implements Runnable {
        private enum ThreadState {
            UNKNOWN,
            RUNNING,
            INTERRUPTED,
            EXITING,
        }

        private final ConcurrentLinkedDeque<Exception> exceptions = new ConcurrentLinkedDeque<>();
        private final FileLock serverLock;
        private final String sshPropertiesHashString;

        public ClientWatcher(final FileLock serverLock, final String sshPropertiesHashString) {
            this.serverLock = serverLock;
            this.sshPropertiesHashString = sshPropertiesHashString;
        }

        @Override
        public void run() {
            ThreadState state = ThreadState.RUNNING;
            try {
                final AtomicInteger clientCount = new AtomicInteger();
                do {
                    clientCount.set(0);
                    DocumentDbSshTunnelLock.runInGlobalLock(
                            sshPropertiesHashString,
                            () -> checkAndHandleClientLocks(clientCount, sshPropertiesHashString, serverLock));
                    if (clientCount.get() > 0) {
                        TimeUnit.MILLISECONDS.sleep(CLIENT_WATCH_POLL_TIME);
                    } else {
                        state = ThreadState.EXITING;
                    }
                } while (state == ThreadState.RUNNING);
            } catch (Exception e) {
                exceptions.add(logException(e));
            } finally {
                try {
                    final Exception localException = DocumentDbSshTunnelLock.runInGlobalLock(
                            sshPropertiesHashString, () -> closeServerLock(serverLock));
                    if (localException != null) {
                        exceptions.add(localException);
                    }
                } catch (Exception e) {
                    exceptions.add(logException(e));
                }
            }
        }

        /**
         * Checks all the client lock files. If any client lock files can be locked, then
         * client has abandoned the file, and it can be deleted. File locks that cannot be attained
         * must be considered alive. If there are no locked files, then we can safely unlock and close the server
         * lock.
         *
         * @param clientCount the number of alive clients with locked files.
         */
        @SneakyThrows
        private static Exception checkAndHandleClientLocks(
                final AtomicInteger clientCount, final String sshPropertiesHashString, final FileLock serverLock) {
            Exception result = null;
            final Path clientsFolderPath = DocumentDbSshTunnelLock.getClientsFolderPath(sshPropertiesHashString);
            Files.createDirectories(clientsFolderPath);
            try (Stream<Path> files = Files.list(clientsFolderPath)) {
                for (Path filePath : files.collect(Collectors.toList())) {
                    final Exception exception = checkClientLock(clientCount, filePath);
                    if (exception != null) {
                        return exception;
                    }
                }
            }
            if (clientCount.get() == 0) {
                result = closeServerLock(serverLock);
            }
            return result;
        }

        /**
         * Checks the client lock for one file path.
         * ASSUMPTION: this method is called from withing a global lock.
         *
         * @param clientCount the number of active clients with locked files.
         * @param filePath    the path to the client lock file.
         */
        private static Exception checkClientLock(final AtomicInteger clientCount, final Path filePath) {
            Exception result = null;
            try (DocumentDbMultiThreadFileChannel fileChannel = DocumentDbMultiThreadFileChannel.open(
                    filePath, StandardOpenOption.WRITE)) {
                final FileLock fileLock = fileChannel.tryLock();
                if (fileLock == null) {
                    clientCount.getAndIncrement();
                } else {
                    fileLock.close();
                    Files.deleteIfExists(filePath);
                }
            } catch (Exception e) {
                result = logException(e);
            }
            return result;
        }

        @NonNull
        public ConcurrentLinkedDeque<Exception> getExceptions() {
            return exceptions;
        }
    }

    private static <T extends Exception> SQLException logException(final T e) {
        LOGGER.error(e.getMessage(), e);
        if (e instanceof SQLException) {
            return (SQLException) e;
        }
        return new SQLException(e.getMessage(), e);
    }

    /**
     * Container for the SSH port forwarding tunnel session.
     */
    @Getter
    @AllArgsConstructor
    static class SshPortForwardingSession {
        /**
         * Gets the SSH session.
         */
        private final Session session;
        /**
         * Gets the local port for the port forwarding tunnel.
         */
        private final int localPort;
    }
}
