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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.ValidationType.SSH_TUNNEL;

class DocumentDbSshTunnelServerTest {

    private final Object mutex = new Object();

    @Test
    @Tag("remote-integration")
    void testAddRemoveClient() throws Exception {
        final String connectionString = DocumentDbSshTunnelClientTest.getConnectionString();
        final DocumentDbConnectionProperties properties =
                DocumentDbConnectionProperties.getPropertiesFromConnectionString(connectionString, SSH_TUNNEL);
        final DocumentDbSshTunnelServer server = DocumentDbSshTunnelServer.builder(
                        properties.getSshUser(),
                        properties.getSshHostname(),
                        properties.getSshPrivateKeyFile(),
                        properties.getHostname())
                .sshStrictHostKeyChecking(properties.getSshStrictHostKeyChecking())
                .build();
        final int timeoutSECS = 1;
        try  {
            server.addClient();
            Assertions.assertTrue(server.getServiceListeningPort() > 0);
            TimeUnit.SECONDS.sleep(timeoutSECS);
            Assertions.assertTrue(server.isAlive());
        } finally {
            server.setCloseDelayMS(0);
            Assertions.assertNotNull(server);
            server.removeClient();
            Assertions.assertEquals(0, server.getServiceListeningPort());
            TimeUnit.SECONDS.sleep(timeoutSECS);
            Assertions.assertFalse(server.isAlive());
            Assertions.assertEquals(0, server.getClientCount());
            // Extra remove is ignored.
            Assertions.assertDoesNotThrow(server::removeClient);
            Assertions.assertFalse(server.isAlive());
            Assertions.assertEquals(0, server.getClientCount());
        }
    }

    @Test
    @Tag("remote-integration")
    void testAddRemoveClientDelayedClose() throws Exception {
        final String connectionString = DocumentDbSshTunnelClientTest.getConnectionString();
        final DocumentDbConnectionProperties properties =
                DocumentDbConnectionProperties.getPropertiesFromConnectionString(connectionString, SSH_TUNNEL);
        final DocumentDbSshTunnelServer server = DocumentDbSshTunnelServer.builder(
                        properties.getSshUser(),
                        properties.getSshHostname(),
                        properties.getSshPrivateKeyFile(),
                        properties.getHostname())
                .sshStrictHostKeyChecking(properties.getSshStrictHostKeyChecking())
                .build();
        final int timeoutSECS = 1;
        try  {
            server.addClient();
            Assertions.assertTrue(server.getServiceListeningPort() > 0);
            TimeUnit.SECONDS.sleep(timeoutSECS);
            Assertions.assertTrue(server.isAlive());
        } finally {
            Assertions.assertNotNull(server);
            final int closeDelayMS = 5000;
            final int closeDelayTimeWithBuffer = closeDelayMS;
            server.setCloseDelayMS(closeDelayMS);
            server.removeClient();
            final Instant expectedCloseTime = Instant.now().plus(closeDelayTimeWithBuffer, ChronoUnit.MILLIS);
            while (Instant.now().isBefore(expectedCloseTime)) {
                Assertions.assertTrue(server.getServiceListeningPort() != 0);
                Assertions.assertTrue(server.isAlive());
                Assertions.assertEquals(0, server.getClientCount());
                TimeUnit.MILLISECONDS.sleep(100);
            }
            TimeUnit.MILLISECONDS.sleep(100);
            Assertions.assertEquals(0, server.getServiceListeningPort());
            TimeUnit.SECONDS.sleep(timeoutSECS);
            Assertions.assertFalse(server.isAlive());
            Assertions.assertEquals(0, server.getClientCount());
            // Extra remove is ignored.
            Assertions.assertDoesNotThrow(server::removeClient);
            Assertions.assertFalse(server.isAlive());
            Assertions.assertEquals(0, server.getClientCount());
        }
    }

    @Test
    @Tag("remote-integration")
    void testAddRemoveClientBeforeDelayedClose() throws Exception {
        final String connectionString = DocumentDbSshTunnelClientTest.getConnectionString();
        final DocumentDbConnectionProperties properties =
                DocumentDbConnectionProperties.getPropertiesFromConnectionString(connectionString, SSH_TUNNEL);
        final DocumentDbSshTunnelServer server = DocumentDbSshTunnelServer.builder(
                        properties.getSshUser(),
                        properties.getSshHostname(),
                        properties.getSshPrivateKeyFile(),
                        properties.getHostname())
                .sshStrictHostKeyChecking(properties.getSshStrictHostKeyChecking())
                .build();
        final int closeDelayMS = 2000;
        final int closeDelayTimeWithBuffer = closeDelayMS;
        final int timeoutSECS = 1;
        try  {
            Assertions.assertNotNull(server);
            server.setCloseDelayMS(closeDelayMS);
            server.addClient();
            Assertions.assertEquals(1, server.getClientCount());
            Assertions.assertTrue(server.getServiceListeningPort() > 0);
            TimeUnit.MILLISECONDS.sleep(closeDelayMS * 2);
            Assertions.assertTrue(server.isAlive());
            server.removeClient();
            Assertions.assertEquals(0, server.getClientCount());
            Assertions.assertTrue(server.isAlive());
            TimeUnit.MILLISECONDS.sleep(closeDelayMS / 2);
            server.addClient();
            Assertions.assertEquals(1, server.getClientCount());
            Assertions.assertTrue(server.isAlive());
            TimeUnit.MILLISECONDS.sleep(closeDelayMS * 2);
            Assertions.assertTrue(server.isAlive());
        } finally {
            Assertions.assertNotNull(server);
            server.setCloseDelayMS(closeDelayMS);
            server.removeClient();
            final Instant expectedCloseTime = Instant.now().plus(closeDelayTimeWithBuffer, ChronoUnit.MILLIS);
            while (Instant.now().isBefore(expectedCloseTime)) {
                Assertions.assertTrue(server.getServiceListeningPort() != 0);
                Assertions.assertTrue(server.isAlive());
                Assertions.assertEquals(0, server.getClientCount());
                TimeUnit.MILLISECONDS.sleep(100);
            }
            TimeUnit.MILLISECONDS.sleep(100);
            Assertions.assertEquals(0, server.getServiceListeningPort());
            TimeUnit.SECONDS.sleep(timeoutSECS);
            Assertions.assertFalse(server.isAlive());
            Assertions.assertEquals(0, server.getClientCount());
            // Extra remove is ignored.
            Assertions.assertDoesNotThrow(server::removeClient);
            Assertions.assertFalse(server.isAlive());
            Assertions.assertEquals(0, server.getClientCount());
        }
    }

    @Test
    @Tag("remote-integration")
    void testAddRemoveClientAfterDelayedClose() throws Exception {
        final String connectionString = DocumentDbSshTunnelClientTest.getConnectionString();
        final DocumentDbConnectionProperties properties =
                DocumentDbConnectionProperties.getPropertiesFromConnectionString(connectionString, SSH_TUNNEL);
        final DocumentDbSshTunnelServer server = DocumentDbSshTunnelServer.builder(
                        properties.getSshUser(),
                        properties.getSshHostname(),
                        properties.getSshPrivateKeyFile(),
                        properties.getHostname())
                .sshStrictHostKeyChecking(properties.getSshStrictHostKeyChecking())
                .build();
        final int closeDelayMS = 2000;
        final int closeDelayTimeWithBuffer = closeDelayMS;
        final int timeoutSECS = 1;
        try  {
            Assertions.assertNotNull(server);
            server.setCloseDelayMS(closeDelayMS);
            server.addClient();
            Assertions.assertEquals(1, server.getClientCount());
            Assertions.assertTrue(server.getServiceListeningPort() > 0);
            TimeUnit.MILLISECONDS.sleep(closeDelayMS * 2);
            Assertions.assertTrue(server.isAlive());
            server.removeClient();
            Assertions.assertEquals(0, server.getClientCount());
            Assertions.assertTrue(server.isAlive());
            TimeUnit.MILLISECONDS.sleep(closeDelayMS * 2);
            Assertions.assertFalse(server.isAlive());
            server.addClient();
            Assertions.assertEquals(1, server.getClientCount());
            Assertions.assertTrue(server.isAlive());
            TimeUnit.MILLISECONDS.sleep(closeDelayMS * 2);
            Assertions.assertTrue(server.isAlive());
        } finally {
            Assertions.assertNotNull(server);
            server.setCloseDelayMS(closeDelayMS);
            server.removeClient();
            final Instant expectedCloseTime = Instant.now().plus(closeDelayTimeWithBuffer, ChronoUnit.MILLIS);
            while (Instant.now().isBefore(expectedCloseTime)) {
                Assertions.assertTrue(server.getServiceListeningPort() != 0);
                Assertions.assertTrue(server.isAlive());
                Assertions.assertEquals(0, server.getClientCount());
                TimeUnit.MILLISECONDS.sleep(100);
            }
            TimeUnit.MILLISECONDS.sleep(100);
            Assertions.assertEquals(0, server.getServiceListeningPort());
            TimeUnit.SECONDS.sleep(timeoutSECS);
            Assertions.assertFalse(server.isAlive());
            Assertions.assertEquals(0, server.getClientCount());
            // Extra remove is ignored.
            Assertions.assertDoesNotThrow(server::removeClient);
            Assertions.assertFalse(server.isAlive());
            Assertions.assertEquals(0, server.getClientCount());
        }
    }

    @Test
    @Tag("remote-integration")
    void testAddRemoveClientMultiThreaded() throws SQLException, InterruptedException {
        final int numOfThreads = 10;
        final List<Thread> threads = new ArrayList<>();
        final List<Runner> runners = new ArrayList<>();
        final String connectionString = DocumentDbSshTunnelClientTest.getConnectionString();
        final DocumentDbConnectionProperties properties =
                DocumentDbConnectionProperties.getPropertiesFromConnectionString(connectionString, SSH_TUNNEL);

        final DocumentDbSshTunnelServer server = DocumentDbSshTunnelServer.builder(
                        properties.getSshUser(),
                        properties.getSshHostname(),
                        properties.getSshPrivateKeyFile(),
                        properties.getHostname())
                .sshStrictHostKeyChecking(properties.getSshStrictHostKeyChecking())
                .build();
        Assertions.assertNotNull(server);
        server.setCloseDelayMS(0);

        // Create all the runners and assign them to a thread.
        for (int i = 0; i < numOfThreads; i++) {
            final int runtimeSecs = numOfThreads - i;
            final Runner runner = new Runner(runtimeSecs, server);
            final Thread threadRunner = new Thread(runner);
            runners.add(runner);
            threads.add(threadRunner);
        }

        // Start all the threads.
        for (int i = 0; i < numOfThreads; i++) {
            threads.get(i).start();
        }

        // Wait for the threads to complete.
        TimeUnit.SECONDS.sleep(1);
        while (threads.size() > 0) {
            TimeUnit.MILLISECONDS.sleep(100);
            synchronized (mutex) {
                // Allow thread to exit after releasing the MUTEX.
                TimeUnit.MILLISECONDS.sleep(10);
                final long clientCount = server.getClientCount();
                int threadCount = 0;
                for (int i = threads.size() - 1; i >= 0; i--) {
                    if (threads.get(i).isAlive()) {
                        threadCount++;
                        Assertions.assertTrue(server.isAlive());
                    } else {
                        threads.get(i).join();
                        threads.remove(i);
                    }
                }
                Assertions.assertEquals(clientCount, threadCount);
                Assertions.assertTrue((clientCount > 0 && server.isAlive()) || !server.isAlive());
            }
        }

        // Ensure no more clients and no longer alive.
        Assertions.assertEquals(0, server.getClientCount());
        Assertions.assertFalse(server.isAlive());

        // Ensure clients didn't throw any exceptions.
        for (final Runner runner : runners) {
            Assertions.assertEquals(0, runner.getExceptions().size(),
                    () -> runner.getExceptions().stream()
                            .map(Throwable::getMessage)
                            .collect(Collectors.joining("; ")));
        }
    }

    private class Runner implements Runnable {
        private final int runtimeSecs;
        private final DocumentDbSshTunnelServer server;
        private final Queue<Exception> exceptions = new ConcurrentLinkedDeque<>();

        public Runner(final int runtimeSecs, final DocumentDbSshTunnelServer server) {
            this.runtimeSecs = runtimeSecs;
            this.server = server;
        }

        public Queue<Exception> getExceptions() {
            return exceptions;
        }

        @Override
        public void run() {
            try {
                synchronized (mutex) {
                    server.addClient();
                }
                TimeUnit.SECONDS.sleep(runtimeSecs);
            } catch (Exception e) {
                exceptions.add(e);
            } finally {
                try {
                    synchronized (mutex) {
                        server.removeClient();
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        }
    }
}
