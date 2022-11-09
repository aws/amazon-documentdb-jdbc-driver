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

import com.google.common.hash.Hashing;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The DocumentDbSshTunnelLock provides various static methods to support
 * file locking for the SSH tunnel implementation.
 */
public final class DocumentDbSshTunnelLock {
    static final String LOCK_BASE_FOLDER_NAME = getLockBaseFolderName();
    static final String PORT_LOCK_NAME = ".sshTunnelLockPort";
    static final String STARTUP_LOCK_NAME = ".sshTunnelLockStartup";
    static final String SERVER_LOCK_NAME = ".sshTunnelLockServer";
    static final String CLIENT_LOCK_FOLDER_NAME = "clients";
    private static final String GLOBAL_LOCK_NAME = ".sshTunnelLockGlobal";
    private static final String CLIENT_LOCK_NAME = ".sshTunnelLockClient";
    private static String classPathLocationName = null;

    private DocumentDbSshTunnelLock() {
        // Empty by design
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
     * Runs a {@link Supplier} method within a "global lock".
     *
     * @param propertiesHashString the SSH properties hash string.
     * @param function the lambda function to execute.
     * @return the value returned from the lambda function.
     * @param <R> the return value type.
     * @throws Exception thrown if an error occurs when attaining the "global lock".
     */
    static <R> R runInGlobalLock(final @NonNull String propertiesHashString, final @NonNull Supplier<R> function)
            throws Exception {
        final Path globalLockPath = getGlobalLockPath(propertiesHashString);
        final Path parentPath = globalLockPath.getParent();
        assert parentPath != null;
        Files.createDirectories(parentPath);
        try (DocumentDbMultiThreadFileChannel globalChannel = DocumentDbMultiThreadFileChannel.open(
                globalLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = globalChannel.lock()) {
            return function.get();
        } // Note: this releases the lock, too.
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    static @NonNull Path getGlobalLockPath(final @NonNull String propertiesHashString) {
        return Paths.get(
                LOCK_BASE_FOLDER_NAME,
                propertiesHashString,
                GLOBAL_LOCK_NAME);
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    static @NonNull Path getServerLockPath(final @NonNull String propertiesHashString) {
        return Paths.get(
                LOCK_BASE_FOLDER_NAME,
                propertiesHashString,
                SERVER_LOCK_NAME
        );
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    static @NonNull Path getStartupLockPath(final @NonNull String sshPropertiesHashString) {
        return Paths.get(
                LOCK_BASE_FOLDER_NAME,
                sshPropertiesHashString,
                STARTUP_LOCK_NAME);
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    static @NonNull Path getPortLockPath(final @NonNull String propertiesHashString) {
        return Paths.get(
                LOCK_BASE_FOLDER_NAME,
                propertiesHashString,
                PORT_LOCK_NAME
        );
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    static @NonNull Path getLockDirectoryPath(final @NonNull String propertiesHashString) {
        return Paths.get(
                LOCK_BASE_FOLDER_NAME,
                propertiesHashString
        );
    }

    static void deleteStartupAndPortLockFiles(
            final @NonNull Path startupLockPath,
            final @NonNull Path portLockPah) throws IOException {
        Files.deleteIfExists(portLockPah);
        Files.deleteIfExists(startupLockPath);
    }

    static void deleteLockDirectory(final @NonNull String propertiesHashString) throws IOException {
        final Path lockDirectoryPath = getLockDirectoryPath(propertiesHashString);
        deleteDirectoryRecursive(lockDirectoryPath);
    }

    private static void deleteDirectoryRecursive(final @NonNull Path directoryPath) throws IOException {
        if (!Files.exists(directoryPath)) {
            return;
        }
        try (Stream<Path> pathStream = Files.walk(directoryPath)) {
            pathStream.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

        }
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    static @NonNull Path getClientLockPath(final @NonNull UUID unique, final @NonNull String propertiesHashString) {
        return Paths.get(
                LOCK_BASE_FOLDER_NAME,
                propertiesHashString,
                CLIENT_LOCK_FOLDER_NAME,
                CLIENT_LOCK_NAME + "-" + unique);
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    static Path getClientsFolderPath(final String sshPropertiesHashString) {
        return Paths.get(
                LOCK_BASE_FOLDER_NAME,
                sshPropertiesHashString,
                CLIENT_LOCK_FOLDER_NAME);
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    private static String getLockBaseFolderName() {
        return Paths.get(
                getDocumentdbHomePathName(),
                "sshTunnelLocks").toString();
    }

    /**
     * Gets the ~/.documentdb path name.
     *
     * @return the ~/.documentdb path name.
     */
    public static String getDocumentdbHomePathName() {
        return DocumentDbConnectionProperties.DOCUMENTDB_HOME_PATH_NAME;
    }

    /**
     * Gets the class path's location name.
     *
     * @return the class path's location name.
     */
    public static String getClassPathLocationName() {
        if (classPathLocationName == null) {
            classPathLocationName = DocumentDbConnectionProperties.getClassPathLocation();
        }
        return classPathLocationName;
    }

    /**
     * Gets the user's home path name.
     *
     * @return the user's home path name.
     */
    static String getUserHomePathName() {
        return DocumentDbConnectionProperties.USER_HOME_PATH_NAME;
    }
}
