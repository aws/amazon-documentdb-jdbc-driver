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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getDocumentDbSearchPaths;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getPath;
import static software.amazon.documentdb.jdbc.sshtunnel.DocumentDbSshTunnelLock.getClassPathLocationName;
import static software.amazon.documentdb.jdbc.sshtunnel.DocumentDbSshTunnelLock.getDocumentdbHomePathName;
import static software.amazon.documentdb.jdbc.sshtunnel.DocumentDbSshTunnelLock.getUserHomePathName;

class DocumentDbSshTunnelServiceTest {
    private static final String DOC_DB_PRIV_KEY_FILE_PROPERTY = "DOC_DB_PRIV_KEY_FILE";
    private static final String DOC_DB_USER_PROPERTY = "DOC_DB_USER";
    private static final String DOC_DB_HOST_PROPERTY = "DOC_DB_HOST";

    @Test
    @Tag("remote-integration")
    @DisplayName("Tests that SSH tunnel service can be started and stays alive while client lock exists.")
    void testRun() throws Exception {
        final String connectionString = getConnectionString();
        String sshPropertiesHashString = null;
        final int waitTimeMS = 2000;

        try (DocumentDbSshTunnelService service = new DocumentDbSshTunnelService(connectionString)) {
            // Ensure the lock directory is empty.
            sshPropertiesHashString = service.getSshPropertiesHashString();
            DocumentDbSshTunnelLock.deleteLockDirectory(sshPropertiesHashString);

            // Prepare to create a client lock.
            final UUID unique = UUID.randomUUID();
            final Path clientLockPath = DocumentDbSshTunnelLock.getClientLockPath(
                    unique, service.getSshPropertiesHashString());
            DocumentDbMultiThreadFileChannel clientChannelCopy = null;
            FileLock clientLockCopy = null;

            try {
                final Path parentPath = clientLockPath.getParent();
                assert parentPath != null;
                Files.createDirectories(parentPath);
                // Create and lock the client lock file.
                final DocumentDbMultiThreadFileChannel clientChannel = DocumentDbMultiThreadFileChannel.open(
                        clientLockPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                final FileLock clientLock = clientChannel.lock();
                clientChannelCopy = clientChannel;
                clientLockCopy = clientLock;

                // Start the service thread and confirm it is alive.
                final Thread serviceThread = startServiceAndValidateIsAlive(service);

                // Close the client lock to signal the service
                final Exception closeException = closeClientLockInGlobalLock(
                        service, clientLockPath, clientChannel, clientLock);
                Assertions.assertNull(closeException);

                // Wait and confirm service has stopped.
                serviceThread.join(waitTimeMS);
                Assertions.assertFalse(serviceThread.isAlive());

                // Validate there are no returned exceptions.
                validateExceptions(service);
            } finally {
                releaseResources(service, clientChannelCopy, clientLockCopy);
            }
        } finally {
            // Clean-up the lock file directory
            assert sshPropertiesHashString != null;
            DocumentDbSshTunnelLock.deleteLockDirectory(sshPropertiesHashString);
        }
    }

    @Test()
    @DisplayName("Tests the getPath method.")
    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    void testGetPath() throws IOException {
        final String tempFilename1 = UUID.randomUUID().toString();

        // Test that it will return using the "current directory"
        final Path path1 = getPath(tempFilename1);
        Assertions.assertEquals(Paths.get(tempFilename1).toAbsolutePath(), path1);

        // Test that it will use the user's home path
        final Path path2 = getPath("~/" + tempFilename1);
        Assertions.assertEquals(Paths.get(getUserHomePathName(), tempFilename1), path2);

        // Test that it will use the user's home path
        Path homeTempFilePath = null;
        try {
            homeTempFilePath = Paths.get(getUserHomePathName(), tempFilename1);
            Assertions.assertTrue(homeTempFilePath.toFile().createNewFile());
            final Path path3 = getPath(tempFilename1, getDocumentDbSearchPaths());
            Assertions.assertEquals(Paths.get(getUserHomePathName(), tempFilename1), path3);
        } finally {
            Assertions.assertTrue(homeTempFilePath != null && homeTempFilePath.toFile().delete());
        }

        // Test that it will use the .documentdb folder under the user's home path
        Path documentDbTempFilePath = null;
        try {
            documentDbTempFilePath = Paths.get(getDocumentdbHomePathName(), tempFilename1);
            final File documentDbDirectory = Paths.get(getDocumentdbHomePathName()).toFile();
            if (!documentDbDirectory.exists()) {
                Assertions.assertTrue(documentDbDirectory.mkdir());
            }
            Assertions.assertTrue(documentDbTempFilePath.toFile().createNewFile());
            final Path path4 = getPath(tempFilename1, getDocumentDbSearchPaths());
            Assertions.assertEquals(Paths.get(getDocumentdbHomePathName(), tempFilename1), path4);
        } finally {
            Assertions.assertTrue(documentDbTempFilePath != null && documentDbTempFilePath.toFile().delete());
        }

        // Test that it will use the .documentdb folder under the user's home path
        Path classPathParentTempFilePath = null;
        try {
            classPathParentTempFilePath = Paths.get(getClassPathLocationName(), tempFilename1);
            Assertions.assertTrue(classPathParentTempFilePath.toFile().createNewFile());
            final Path path5 = getPath(tempFilename1, getDocumentDbSearchPaths());
            Assertions.assertEquals(Paths.get(getClassPathLocationName(), tempFilename1), path5);
        } finally {
            Assertions.assertTrue(classPathParentTempFilePath != null && classPathParentTempFilePath.toFile().delete());
        }

        // Test that will recognize and use an absolute path
        File tempFile = null;
        try {
            tempFile = File.createTempFile("documentdb", ".tmp");
            final Path path5 = getPath(tempFile.getAbsolutePath());
            Assertions.assertEquals(Paths.get(tempFile.getAbsolutePath()), path5);
        } finally {
            Assertions.assertTrue(tempFile != null && tempFile.delete());
        }
    }

    private static void releaseResources(
            final DocumentDbSshTunnelService service,
            final DocumentDbMultiThreadFileChannel clientChannelCopy,
            final FileLock clientLockCopy) throws Exception {
        if (clientLockCopy != null && clientLockCopy.isValid()) {
            clientLockCopy.close();
        }
        if (clientChannelCopy != null && clientChannelCopy.isOpen()) {
            clientChannelCopy.close();
        }
        if (service != null) {
            service.close();
        }
    }

    private static Thread startServiceAndValidateIsAlive(final DocumentDbSshTunnelService service)
            throws InterruptedException {
        final int waitTimeMS = 2000;
        final Thread serviceThread = startThread(service);
        serviceThread.join(waitTimeMS);
        validateExceptions(service);
        Assertions.assertTrue(serviceThread.isAlive());
        validateExceptions(service);
        serviceThread.join(waitTimeMS);
        Assertions.assertTrue(serviceThread.isAlive());
        return serviceThread;
    }

    private static Exception closeClientLockInGlobalLock(
            final DocumentDbSshTunnelService service,
            final Path clientLockPath,
            final DocumentDbMultiThreadFileChannel clientChannel,
            final FileLock clientLock) throws Exception {
        return DocumentDbSshTunnelLock.runInGlobalLock(
                service.getSshPropertiesHashString(),
                () -> {
                    Exception exception = null;
                    try {
                        clientLock.close();
                        clientChannel.close();
                        Files.deleteIfExists(clientLockPath);
                    } catch (Exception e) {
                        exception = e;
                    }
                    return exception;
                });
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

    private static Thread startThread(final DocumentDbSshTunnelService service) {
        final Thread serviceThread = new Thread(service);
        serviceThread.setDaemon(true);
        serviceThread.start();
        return serviceThread;
    }

    private static void validateExceptions(final DocumentDbSshTunnelService service) {
        if (service.getExceptions().size() != 0) {
            for (Exception e : service.getExceptions()) {
                Assertions.assertInstanceOf(Exception.class, e);
                System.out.println(e.toString());
                for (StackTraceElement stackLine : e.getStackTrace()) {
                    System.out.println(stackLine.toString());
                }
            }
            Assertions.fail();
        }
    }
}
