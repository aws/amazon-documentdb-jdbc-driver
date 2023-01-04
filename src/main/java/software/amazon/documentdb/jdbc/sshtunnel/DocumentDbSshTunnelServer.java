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
import com.google.common.hash.Hashing;
import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getPath;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.isNullOrWhitespace;

/**
 * Provides a single-instance SSH Tunnel server.
 * <p>
 * Use the {@link #builder(String, String, String, String)} method to instantiate
 * a new {@link DocumentDbSshTunnelServerBuilder} object. Set the properties as needed,
 * then call the build() method.
 */
public final class DocumentDbSshTunnelServer implements AutoCloseable {
    public static final String SSH_KNOWN_HOSTS_FILE = "~/.ssh/known_hosts";
    public static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    public static final String HASH_KNOWN_HOSTS = "HashKnownHosts";
    public static final String SERVER_HOST_KEY = "server_host_key";
    public static final String YES = "yes";
    public static final String NO = "no";
    public static final String LOCALHOST = "localhost";
    public static final int DEFAULT_DOCUMENTDB_PORT = 27017;
    public static final int DEFAULT_SSH_PORT = 22;
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSshTunnelServer.class);
    public static final int DEFAULT_CLOSE_DELAY_MS = 30000;

    private final Object mutex = new Object();
    private final AtomicLong clientCount = new AtomicLong(0);

    private final String sshUser;
    private final String sshHostname;
    private final String sshPrivateKeyFile;
    private final String sshPrivateKeyPassphrase;
    private final boolean sshStrictHostKeyChecking;
    private final String sshKnownHostsFile;
    private final String remoteHostname;
    private final  ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private DocumentDbSshTunnelServer.SshPortForwardingSession session = null;
    private ScheduledFuture<?> scheduledFuture = null;
    private long closeDelayMS = DEFAULT_CLOSE_DELAY_MS;


    private DocumentDbSshTunnelServer(final DocumentDbSshTunnelServerBuilder builder) {
        this.sshUser = builder.sshUser;
        this.sshHostname = builder.sshHostname;
        this.sshPrivateKeyFile = builder.sshPrivateKeyFile;
        this.remoteHostname = builder.sshRemoteHostname;
        this.sshPrivateKeyPassphrase = builder.sshPrivateKeyPassphrase;
        this.sshStrictHostKeyChecking = builder.sshStrictHostKeyChecking;
        this.sshKnownHostsFile = builder.sshKnownHostsFile;
        LOGGER.debug("sshUser='{}' sshHostname='{}' sshPrivateKeyFile='{}' remoteHostname'{}"
                + " sshPrivateKeyPassphrase='{}' sshStrictHostKeyChecking='{}' sshKnownHostsFile='{}'",
                this.sshUser,
                this.sshHostname,
                this.sshPrivateKeyFile,
                this.remoteHostname,
                this.sshPrivateKeyPassphrase,
                this.sshStrictHostKeyChecking,
                this.sshKnownHostsFile
        );
    }

    /**
     * Gets the hash string for the SSH properties provided.
     *
     * @param sshUser the username credential for the SSH tunnel.
     * @param sshHostname the hostname (or IP address) for the SSH tunnel.
     * @param sshPrivateKeyFile the path to the private key file.
     *
     * @return a String value representing the hash of the given properties.
     */
    static String getHashString(
            final String sshUser,
            final String sshHostname,
            final String sshPrivateKeyFile,
            final String remoteHostname) {
        final String sshPropertiesString = sshUser + "-" + sshHostname + "-" + sshPrivateKeyFile + remoteHostname;
        return Hashing.sha256()
                .hashString(sshPropertiesString, StandardCharsets.UTF_8)
                .toString();
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
        validateSshPrivateKeyFile(connectionProperties);

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
            LOGGER.debug("Internal SSH tunnel started.");
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
        final String knownHostsFilename;
        knownHostsFilename = getSshKnownHostsFilename(connectionProperties);
        try {
            jSch.setKnownHosts(knownHostsFilename);
        } catch (JSchException e) {
            throw logException(e);
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
     * Gets the SSH tunnel service listening port. A value of zero indicates that the
     * SSH tunnel service is not running.
     *
     * @return A port number that the SSH tunnel service is listening on.
     */
    public int getServiceListeningPort() {
        return session != null ? session.getLocalPort() : 0;
    }

    @Override
    public void close() {
        synchronized (mutex) {
            if (session != null) {
                LOGGER.debug("Internal SSH Tunnel is stopping.");
                session.getSession().disconnect();
                session = null;
                LOGGER.debug("Internal SSH Tunnel is stopped.");
            }
        }
    }

    /**
     * Adds a client to the reference count for this server. If this is the first client, the server
     * ensures that an SSH Tunnel service is started.
     *
     * @throws SQLException When an error occurs trying to start the SSH Tunnel service.
     * @throws InterruptedException When a scheduled task is interrupted.
     */
    public void addClient() throws SQLException, InterruptedException {
        // Needs to be synchronized in a single process
        synchronized (mutex) {
            if (scheduledFuture != null) {
                cancelScheduledFutureClose();
            }
            clientCount.incrementAndGet();
            if (session != null && session.getLocalPort() != 0) {
                return;
            }
            validateLocalSshFilesExists();
            session = createSshTunnel(getConnectionProperties());
        }
    }

    /**
     * Removes a client from the reference count for this server. If the reference count reaches zero, then
     * the serve attempt to stop the SSH Tunnel service.
     *
     * @throws SQLException When an error occur attempting shutdown of the service process.
     */
    public void removeClient() throws SQLException {
        synchronized (mutex) {
            if (clientCount.get() <= 0 || clientCount.decrementAndGet() > 0) {
                return;
            }
            closeSession();
        }
    }

    private void closeSession() throws SQLException {
        if (scheduledFuture != null) {
            cancelScheduledFutureClose();
        }
        // Delay the close, if indicated.
        final long delayMS = getCloseDelayMS();
        if (delayMS <= 0) {
            close();
        } else {
            LOGGER.debug("Close timer is being scheduled.");
            scheduledFuture = scheduler.schedule(getCloseTimerTask(), delayMS, TimeUnit.MILLISECONDS);
        }
    }

    private TimerTask getCloseTimerTask() {
        return new TimerTask() {
            @Override
            public void run() {
                try {
                    close();
                } catch (Exception e) {
                    // Ignore exception on close.
                    LOGGER.warn(e.getMessage(), e);
                }
            }
        };
    }

    private void cancelScheduledFutureClose() throws SQLException {
        synchronized (mutex) {
            LOGGER.debug("Close timer is being cancelled.");
            while (!scheduledFuture.isDone()) {
                scheduledFuture.cancel(false);
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    throw new SQLException(e.getMessage(), e);
                }
            }
            scheduledFuture = null;
        }
    }

    long getCloseDelayMS() {
        return closeDelayMS;
    }

    void setCloseDelayMS(final long closeDelayMS) {
        this.closeDelayMS = closeDelayMS >= 0 ? closeDelayMS : 0;
    }

    /**
     * Gets the number of clients using the server.
     *
     * @return The number of clients using the server.
     */
    @VisibleForTesting
    long getClientCount() {
        synchronized (mutex) {
            return clientCount.get();
        }
    }

    /**
     * Checks the state of the SSH tunnel service.
     *
     * @return Returns true if the SSH tunnel service is running.
     */
    public boolean isAlive() {
        return session != null;
    }

    /**
     * Factory method for the {@link DocumentDbSshTunnelServerBuilder} class.
     *
     * @param user the SSH tunnel username.
     * @param hostname the SSH tunnel hostname.
     * @param privateKeyFile the SSH tunnel private key file path.
     * @param remoteHostname the hostname of the remote server.
     *
     * @return a new {@link DocumentDbSshTunnelServerBuilder} instance.
     */
    public static DocumentDbSshTunnelServerBuilder builder(
            final String user,
            final String hostname,
            final String privateKeyFile,
            final String remoteHostname) {
        return new DocumentDbSshTunnelServerBuilder(user, hostname, privateKeyFile, remoteHostname);
    }

    /**
     * The {@link DocumentDbSshTunnelServer} builder class.
     * A call to the {@link #build()} method returns the single instance with
     * the matching SSH tunnel properties.
     */
    public static class DocumentDbSshTunnelServerBuilder {
        private final String sshUser;
        private final String sshHostname;
        private final String sshPrivateKeyFile;
        private final String sshRemoteHostname;
        private String sshPrivateKeyPassphrase = null;
        private boolean sshStrictHostKeyChecking = true;
        private String sshKnownHostsFile = null;

        private static final ConcurrentMap<String, DocumentDbSshTunnelServer> SSH_TUNNEL_MAP =
                new ConcurrentHashMap<>();

        /**
         * A builder class for the DocumentDbSshTunnelServer.
         *
         * @param sshUser the SSH tunnel username.
         * @param sshHostname the SSH tunnel hostname.
         * @param sshPrivateKeyFile the SSH tunnel private key file path.
         * @param sshRemoteHostname the hostname of the remote server.
         */
        DocumentDbSshTunnelServerBuilder(
                final String sshUser,
                final String sshHostname,
                final String sshPrivateKeyFile,
                final String sshRemoteHostname) {
            this.sshUser = sshUser;
            this.sshHostname = sshHostname;
            this.sshPrivateKeyFile = sshPrivateKeyFile;
            this.sshRemoteHostname = sshRemoteHostname;
        }

        /**
         * Sets the private key passphrase.
         *
         * @param sshPrivateKeyPassphrase the private key passphrase.
         * @return the current instance of the builder.
         */
        public DocumentDbSshTunnelServerBuilder sshPrivateKeyPassphrase(final String sshPrivateKeyPassphrase) {
            this.sshPrivateKeyPassphrase = sshPrivateKeyPassphrase;
            return this;
        }

        /**
         * Sets the strict host key checking option.
         *
         * @param sshStrictHostKeyChecking indicator of whether to set the strict host key checking option.
         * @return the current instance of the builder.
         */
        public DocumentDbSshTunnelServerBuilder sshStrictHostKeyChecking(final boolean sshStrictHostKeyChecking) {
            this.sshStrictHostKeyChecking = sshStrictHostKeyChecking;
            return this;
        }

        /**
         * Sets the known hosts file property.
         *
         * @param sshKnownHostsFile the file path to the known hosts file.
         *
         * @return the current instance of the builder.
         */
        public DocumentDbSshTunnelServerBuilder sshKnownHostsFile(final String sshKnownHostsFile) {
            this.sshKnownHostsFile = sshKnownHostsFile;
            return this;
        }

        /**
         * Builds a DocumentDbSshTunnelServer from the given properties.
         *
         * @return a new instance of DocumentDbSshTunnelServer.
         */
        public DocumentDbSshTunnelServer build() {
            final String hashString = getHashString(
                    this.sshUser,
                    this.sshHostname,
                    this.sshPrivateKeyFile,
                    this.sshRemoteHostname
            );
            // Returns single instance of server for the hashed properties.
            return SSH_TUNNEL_MAP.computeIfAbsent(
                    hashString,
                    key -> new DocumentDbSshTunnelServer(this)
            );
        }
    }

    @NonNull
    private DocumentDbConnectionProperties getConnectionProperties() {
        final DocumentDbConnectionProperties connectionProperties = new DocumentDbConnectionProperties();
        connectionProperties.setHostname(remoteHostname);
        connectionProperties.setSshUser(sshUser);
        connectionProperties.setSshHostname(sshHostname);
        connectionProperties.setSshPrivateKeyFile(sshPrivateKeyFile);
        connectionProperties.setSshStrictHostKeyChecking(String.valueOf(sshStrictHostKeyChecking));
        if (sshPrivateKeyPassphrase != null) {
            connectionProperties.setSshPrivateKeyPassphrase(sshPrivateKeyPassphrase);
        }
        if (sshKnownHostsFile != null) {
            connectionProperties.setSshKnownHostsFile(sshKnownHostsFile);
        }
        return connectionProperties;
    }

    private void validateLocalSshFilesExists() throws SQLException {
        final DocumentDbConnectionProperties connectionProperties = getConnectionProperties();
        validateSshPrivateKeyFile(connectionProperties);
        getSshKnownHostsFilename(connectionProperties);
    }

    static void validateSshPrivateKeyFile(final DocumentDbConnectionProperties connectionProperties)
            throws SQLException {
        if (!connectionProperties.isSshPrivateKeyFileExists()) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.SSH_PRIVATE_KEY_FILE_NOT_FOUND,
                    connectionProperties.getSshPrivateKeyFile());
        }
    }

    static String getSshKnownHostsFilename(final DocumentDbConnectionProperties connectionProperties)
            throws SQLException {
        final String knowHostsFilename;
        if (!isNullOrWhitespace(connectionProperties.getSshKnownHostsFile())) {
            final Path knownHostsPath = getPath(connectionProperties.getSshKnownHostsFile());
            validateSshKnownHostsFile(connectionProperties, knownHostsPath);
            knowHostsFilename = knownHostsPath.toString();
        } else {
            knowHostsFilename = getPath(SSH_KNOWN_HOSTS_FILE).toString();
        }
        return knowHostsFilename;
    }

    private static void validateSshKnownHostsFile(
            final DocumentDbConnectionProperties connectionProperties,
            final Path knownHostsPath) throws SQLException {
        if (!Files.exists(knownHostsPath)) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.INVALID_PARAMETER_VALUE,
                    SqlError.KNOWN_HOSTS_FILE_NOT_FOUND,
                    connectionProperties.getSshKnownHostsFile());
        }
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
