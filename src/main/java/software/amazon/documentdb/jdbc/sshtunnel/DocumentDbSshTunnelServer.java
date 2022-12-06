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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.DocumentDbMain;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.common.utilities.SqlState;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSshTunnelServer.class);
    private static final int SERVER_WATCHER_POLL_TIME_MS = 500;
    private static final Object MUTEX = new Object();
    private static final String DOCUMENTDB_SSH_TUNNEL_PATH = "DOCUMENTDB_SSH_TUNNEL_PATH";
    private static final String JAVA_HOME = "java.home";
    private static final String JAVA_CLASS_PATH = "java.class.path";
    private static final String CLASS_PATH_OPTION_NAME = "-cp";
    private static final String BIN_FOLDER_NAME = "bin";
    private static final String JAVA_EXECUTABLE_NAME = "java";
    private static final String SSH_TUNNEL_SERVICE_OPTION_NAME = "--" + DocumentDbMain.SSH_TUNNEL_SERVICE_OPTION_NAME;
    public static final int SERVICE_WAIT_TIMEOUT_SECONDS = 120;
    public static final String FILE_SCHEME = "file";

    private final AtomicInteger clientCount = new AtomicInteger(0);

    private final String sshUser;
    private final String sshHostname;
    private final String sshPrivateKeyFile;
    private final String sshPrivateKeyPassphrase;
    private final boolean sshStrictHostKeyChecking;
    private final String sshKnownHostsFile;
    private final String remoteHostname;
    private final String propertiesHashString;
    private final AtomicBoolean serverAlive = new AtomicBoolean(false);
    private ServerWatcher serverWatcher = null;
    private Thread serverWatcherThread = null;

    private volatile int serviceListeningPort = 0;

    private DocumentDbSshTunnelServer(final DocumentDbSshTunnelServerBuilder builder) {
        this.sshUser = builder.sshUser;
        this.sshHostname = builder.sshHostname;
        this.sshPrivateKeyFile = builder.sshPrivateKeyFile;
        this.remoteHostname = builder.sshRemoteHostname;
        this.sshPrivateKeyPassphrase = builder.sshPrivateKeyPassphrase;
        this.sshStrictHostKeyChecking = builder.sshStrictHostKeyChecking;
        this.sshKnownHostsFile = builder.sshKnownHostsFile;
        this.propertiesHashString = DocumentDbSshTunnelLock.getHashString(
                sshUser, sshHostname, sshPrivateKeyFile, remoteHostname);
        LOGGER.info("sshUser='{}' sshHostname='{}' sshPrivateKeyFile='{}' remoteHostname'{}"
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
     * Gets the SSH tunnel service listening port. A value of zero indicates that the
     * SSH tunnel service is not running.
     *
     * @return A port number that the SSH tunnel service is listening on.
     */
    public int getServiceListeningPort() {
        return serviceListeningPort;
    }

    @Override
    public void close() throws Exception {
        synchronized (MUTEX) {
            serviceListeningPort = 0;
            shutdownServerWatcherThread();
        }
    }

    /**
     * Adds a client to the reference count for this server. If this is the first client, the server
     * ensures that an SSH Tunnel service is started.
     *
     * @return The current client reference count after adding the new client.
     * @throws Exception When an error occurs trying to start the SSH Tunnel service.
     */
    public int addClient() throws Exception {
        synchronized (MUTEX) {
            if (clientCount.get() == 0) {
                ensureStarted();
            }
            return clientCount.incrementAndGet();
        }
    }

    /**
     * Removes a client from the reference count for this server. If the reference count reaches zero, then
     * the serve attempt to stop the SSH Tunnel service.
     *
     * @return The current client count after removing the client from the reference count.
     * @throws Exception When an error occur attempting shutdown of the service process.
     */
    public int removeClient() throws Exception {
        synchronized (MUTEX) {
            final int currentCount = clientCount.decrementAndGet();
            if (clientCount.get() == 0) {
                close();
            }
            return currentCount;
        }
    }

    private void shutdownServerWatcherThread() throws Exception {
        if (serverWatcherThread.isAlive()) {
            LOGGER.info("Stopping server watcher thread.");
            serverWatcher.close();
            do {
                serverWatcherThread.join(SERVER_WATCHER_POLL_TIME_MS * 2);
            } while (serverWatcherThread.isAlive());
            LOGGER.info("Stopped server watcher thread.");
        } else {
            LOGGER.info("Server watcher thread already stopped.");
        }
    }

    /**
     * Checks the state of the SSH tunnel service.
     *
     * @return Returns true if the SSH tunnel service is running.
     */
    public boolean isAlive() throws Exception {
        if (serverWatcherThread.isAlive()) {
            // While the watcher thread is running, the status should be pretty accurate.
            return serverAlive.get();
        } else {
            // Can no longer rely on watcher to have an updated status, check synchronously here.
            final Path serverLockPath = DocumentDbSshTunnelLock.getServerLockPath(propertiesHashString);
            try (DocumentDbMultiThreadFileChannel serverChannel = DocumentDbMultiThreadFileChannel.open(
                    serverLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                // NOTE: Server lock will be release when channel is closed.
                final FileLock serverLock = serverChannel.tryLock();
                if (serverLock != null) {
                    return false;
                }
            }
            return true;
        }
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
            final String hashString = DocumentDbSshTunnelLock.getHashString(
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

    /**
     * Ensure the service is started.
     */
    void ensureStarted() throws Exception {
        maybeStart();
    }

    // Needs to be synchronized in a single process
    private void maybeStart() throws Exception {
        synchronized (MUTEX) {
            if (serviceListeningPort != 0) {
                return;
            }
            final AtomicReference<Exception> exception = new AtomicReference<>(null);
            DocumentDbSshTunnelLock.runInGlobalLock(
                    propertiesHashString,
                    () -> maybeStartServerHandleException(exception));
            if (exception.get() != null) {
                throw exception.get();
            }
        }
    }

    private Exception maybeStartServerHandleException(final AtomicReference<Exception> exception) {
        try {
            maybeStartServer();
            return null;
        } catch (Exception e) {
            exception.set(e);
            return e;
        }
    }

    private void maybeStartServer() throws Exception {
        final Path serverLockPath = DocumentDbSshTunnelLock.getServerLockPath(propertiesHashString);
        try (DocumentDbMultiThreadFileChannel serverChannel = DocumentDbMultiThreadFileChannel.open(
                serverLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            // NOTE: Server lock will be release when channel is closed.
            FileLock serverLock = serverChannel.tryLock();
            if (serverLock != null) {
                validateLocalSshFilesExists();

                // This indicates that the SSH tunnel service does not have a lock.
                // So startup the SSH tunnel service and read the listening port.
                final Path startupLockPath = DocumentDbSshTunnelLock.getStartupLockPath(propertiesHashString);
                final Path portLockPath = DocumentDbSshTunnelLock.getPortLockPath(propertiesHashString);
                DocumentDbSshTunnelLock.deleteStartupAndPortLockFiles(startupLockPath, portLockPath);

                // Release the server lock file, which is safe since we're in the global lock.
                if (serverLock.isValid()) {
                    serverLock.close();
                }
                // Start the service process
                final Process process = startSshTunnelServiceProcess();
                // Read the listening port
                waitForStartupAndReadPort(startupLockPath, process);
                // Wait for the service to lock the server lock fle.
                final Instant timeoutTime = Instant.now().plus(Duration.ofSeconds(SERVICE_WAIT_TIMEOUT_SECONDS));
                do {
                    serverLock = serverChannel.tryLock();
                    if (serverLock != null && serverLock.isValid()) {
                        serverLock.close();
                        // Ensure we don't wait forever.
                        throwIfTimeout(timeoutTime, "Timeout waiting for service to acquire server lock.");
                        TimeUnit.MILLISECONDS.sleep(SERVER_WATCHER_POLL_TIME_MS);
                    }
                } while (serverLock != null);

                // Now it's safe to start the watcher thread.
                startServerWatcherThread();
            } else {
                // This indicates that the SSH tunnel service does have a lock.
                // So just read the listening port.
                LOGGER.info("Server already running.");
                readSshPortFromFile();
                // Now it's safe to start the watcher thread.
                startServerWatcherThread();
            }
        }
    }

    private static void throwIfTimeout(final Instant timeoutTime, final String message) throws SQLException {
        if (Instant.now().isAfter(timeoutTime)) {
            throw SqlError.createSQLException(LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.SSH_TUNNEL_ERROR,
                    message);
        }
    }

    private void startServerWatcherThread() {
        serverWatcher = new ServerWatcher(propertiesHashString, serverAlive);
        serverWatcherThread = new Thread(serverWatcher);
        serverWatcherThread.setDaemon(true);
        serverWatcherThread.start();
    }

    private void waitForStartupAndReadPort(final Path startupLockPath, final Process process) throws Exception {
        final int pollTimeMS = 100;
        while (!Files.exists(startupLockPath)) {
            TimeUnit.MILLISECONDS.sleep(pollTimeMS);
        }
        try (DocumentDbMultiThreadFileChannel startupChannel = DocumentDbMultiThreadFileChannel.open(
                startupLockPath, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            FileLock startupLock;
            LOGGER.info("Waiting for server to unlock Startup lock file.");
            final Instant timeoutTime = Instant.now().plus(Duration.ofSeconds(SERVICE_WAIT_TIMEOUT_SECONDS));
            do {

                startupLock = startupChannel.tryLock();
                if (startupLock == null) {
                    throwIfProcessHasExited(process);
                    // Ensure we don't wait forever.
                    throwIfTimeout(timeoutTime, "Timeout waiting for service to release Startup lock.");
                    TimeUnit.MILLISECONDS.sleep(pollTimeMS);
                }
            } while (startupLock == null);
            LOGGER.info("Server has unlocked Startup lock file.");
            LOGGER.info("Reading Startup lock file.");

            try (InputStream inputStream = Channels.newInputStream(startupChannel.getFileChannel());
                 InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(streamReader)) {
                final StringBuilder exceptionMessage = new StringBuilder();
                boolean isFirstLine = true;
                String line;
                while ((line = reader.readLine()) != null && !DocumentDbConnectionProperties.isNullOrWhitespace(line)) {
                    if (!isFirstLine) {
                        exceptionMessage.append(System.lineSeparator());
                    } else {
                        isFirstLine = false;
                    }
                    exceptionMessage.append(line);
                }
                if (exceptionMessage.length() > 0) {
                    exceptionMessage.insert(0, "Server exception detected: '").append("'");
                    throw SqlError.createSQLException(
                            LOGGER,
                            SqlState.CONNECTION_EXCEPTION,
                            SqlError.SSH_TUNNEL_ERROR,
                            exceptionMessage.toString());
                }
                LOGGER.info("Finished reading Startup lock file.");
            }
            LOGGER.info("Reading local port number from file.");
            readSshPortFromFile();
        }
    }

    private static void throwIfProcessHasExited(final Process process) throws InterruptedException, SQLException {
        synchronized (process) {
            if (process.waitFor(1, TimeUnit.MILLISECONDS)) {
                throw SqlError.createSQLException(LOGGER,
                        SqlState.CONNECTION_EXCEPTION,
                        SqlError.SSH_TUNNEL_ERROR,
                        "Service has unexpected exited.");
            }
        }
    }

    private Process startSshTunnelServiceProcess()
            throws IOException, SQLException, InterruptedException, URISyntaxException {
        final List<String> command = getSshTunnelCommand();
        final ProcessBuilder builder = new ProcessBuilder(command);
        return builder.inheritIO().start();
    }

    private List<String> getSshTunnelCommand() throws SQLException, IOException, InterruptedException, URISyntaxException {
        final List<String> command = new LinkedList<>();
        final String docDbSshTunnelPathString = System.getenv(DOCUMENTDB_SSH_TUNNEL_PATH);
        if (docDbSshTunnelPathString != null) {
            // NOTE: This is the entry point for the ODBC driver to provide a (full) path to the executable.
            // It is assumed that we will still provide all the arguments to the executable.
            // E.g., on Windows:
            // DOCUMENTDB_SSH_TUNNEL_PATH=C:\Program Files\documentdb-ssh-tunnel-service\documentdb-ssh-tunnel-service.exe
            final Path docDbSshTunnelPath = Paths.get(docDbSshTunnelPathString);
            if (!Files.isExecutable(docDbSshTunnelPath) || Files.isDirectory(docDbSshTunnelPath)) {
                throw SqlError.createSQLException(
                        LOGGER,
                        SqlState.CONNECTION_EXCEPTION,
                        SqlError.SSH_TUNNEL_PATH_NOT_FOUND,
                        docDbSshTunnelPathString);
            }

            command.add(docDbSshTunnelPath.toAbsolutePath().toString());
            command.add(SSH_TUNNEL_SERVICE_OPTION_NAME);
            command.add(getSshPropertiesString());
        } else {
            final String className = DocumentDbMain.class.getName();
            final String sshConnectionProperties = getSshPropertiesString();
            command.addAll(getJavaCommand(className, SSH_TUNNEL_SERVICE_OPTION_NAME, sshConnectionProperties));
        }
        return command;
    }

    /**
     * Gets the command line parameters for invoking the Java executable in the JAVA_HOME.
     *
     * @param className the name of the class that contains the 'main()' method.
     * @param arguments the arguments to the main method.
     * @return a list of arguments to pass to {@link ProcessBuilder}.
     * @throws SQLException when the path to the java executable cannot be resolved.
     */
    public static List<String> getJavaCommand(final String className, final String... arguments)
            throws SQLException, URISyntaxException {
        final String javaBinFilePath = getJavaBinFilePath();
        final String combinedClassPath = getCombinedClassPath();

        final List<String> command = new LinkedList<>();
        command.add(javaBinFilePath);
        command.add(CLASS_PATH_OPTION_NAME);
        command.add(combinedClassPath);
        command.add(className);
        command.addAll(Arrays.asList(arguments));
        return command;
    }

    private static String getJavaBinFilePath() throws SQLException {
        // Check that the java command executable is available relative to the
        // JAVA_HOME environment variable.
        final String javaHome = getJavaHome();
        final String javaBinFilePath = Paths.get(javaHome, BIN_FOLDER_NAME, JAVA_EXECUTABLE_NAME).toString();
        final boolean isOsWindows = org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
        final Path javaBinPath = Paths.get(javaBinFilePath + (isOsWindows ? ".exe" : ""));
        if (!Files.exists(javaBinPath) || !Files.isExecutable(javaBinPath)) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.MISSING_JAVA_BIN,
                    javaBinPath.toString());
        }
        return javaBinFilePath;
    }

    private static String getJavaHome() throws SQLException {
        final String javaHome = System.getProperty(JAVA_HOME);
        if (isNullOrWhitespace(javaHome) || !Files.exists(Paths.get(javaHome))) {
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.MISSING_JAVA_HOME);
        }
        return javaHome;
    }

    private static String getCombinedClassPath() throws URISyntaxException {
        URI currentClassPathUri = DocumentDbSshTunnelServer.class
                .getProtectionDomain().getCodeSource().getLocation().toURI();
        final String schemeSpecificPart = currentClassPathUri.getSchemeSpecificPart();
        if (currentClassPathUri.getScheme().equalsIgnoreCase(FILE_SCHEME)
                && !isNullOrWhitespace(schemeSpecificPart)) {
            // Ensure only 1 slash at beginning.
            final String startsWithSlashExpression = "^/+";
            currentClassPathUri = new URI(currentClassPathUri.getScheme()
                    + ":/"
                    + schemeSpecificPart.replaceAll(startsWithSlashExpression, ""));

        }
        final String currentClassCodeSourcePath = new File(currentClassPathUri).getAbsolutePath();
        return currentClassCodeSourcePath + ";" + System.getProperty(JAVA_CLASS_PATH);
    }

    private @NonNull String getSshPropertiesString() {
        final DocumentDbConnectionProperties connectionProperties = getConnectionProperties();
        return DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME + connectionProperties.buildSshConnectionString();
    }

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
            knowHostsFilename = getPath(DocumentDbSshTunnelService.SSH_KNOWN_HOSTS_FILE).toString();
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

    private void readSshPortFromFile() throws IOException, SQLException {
        final Path portLockPath = DocumentDbSshTunnelLock.getPortLockPath(propertiesHashString);
        final List<String> lines = Files.readAllLines(portLockPath, StandardCharsets.UTF_8);
        int port = 0;
        for (String line : lines) {
            if (!line.trim().isEmpty()) {
                port = Integer.parseInt(line.trim());
                if (port > 0) {
                    break;
                }
            }
        }
        if (port <= 0) {
            serviceListeningPort = 0;
            throw SqlError.createSQLException(
                    LOGGER,
                    SqlState.CONNECTION_EXCEPTION,
                    SqlError.SSH_TUNNEL_ERROR,
                    "Unable to read valid listening port for SSH Tunnel service.");
        }
        serviceListeningPort = port;
        LOGGER.info("SHH tunnel service listening on port: " + serviceListeningPort);
    }

    private enum ServerWatcherState {
        INITIALIZED,
        RUNNING,
        INTERRUPTED,
        COMPLETED,
        ERROR,
    }

    private static class ServerWatcher implements Runnable, AutoCloseable {

        private volatile ServerWatcherState state = ServerWatcherState.INITIALIZED;
        private final Queue<Exception> exceptions = new ConcurrentLinkedDeque<>();
        private final String propertiesHashString;
        private final AtomicBoolean serverAlive;

        ServerWatcher(final String propertiesHashString, final AtomicBoolean serverAlive) {
            this.propertiesHashString = propertiesHashString;
            this.serverAlive = serverAlive;
        }

        public ServerWatcherState getState() {
            return state;
        }

        public List<Exception> getExceptions() {
            return Collections.unmodifiableList(new ArrayList<>(exceptions));
        }

        @Override
        public void run() {
            try {
                state = ServerWatcherState.RUNNING;
                do {
                    DocumentDbSshTunnelLock.runInGlobalLock(propertiesHashString, this::checkForServerLock);
                    if (state == ServerWatcherState.RUNNING) {
                        TimeUnit.MILLISECONDS.sleep(SERVER_WATCHER_POLL_TIME_MS);
                    }
                } while (state == ServerWatcherState.RUNNING);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        @Override
        public void close() throws Exception {
            state = ServerWatcherState.INTERRUPTED;
        }

        private Exception checkForServerLock() {
            Exception exception = null;
            final Path serverLockPath = DocumentDbSshTunnelLock.getServerLockPath(propertiesHashString);
            try (DocumentDbMultiThreadFileChannel serverChannel = DocumentDbMultiThreadFileChannel.open(
                    serverLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                // NOTE: Server lock will be release when channel is closed.
                final FileLock serverLock = serverChannel.tryLock();
                if (serverLock != null) {
                    // Server abandoned lock. Set to false if the previous value was true.
                    serverAlive.compareAndSet(true, false);
                    state = ServerWatcherState.COMPLETED;
                } else {
                    // Server is still alive. Set to true if the previous value was false.
                    serverAlive.compareAndSet(false, true);
                }
            } catch (Exception e) {
                exception = e;
                exceptions.add(e);
                state = ServerWatcherState.ERROR;
            }
            return exception;
        }
    }
}
