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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DocumentDbSshTunnelClientRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbSshTunnelClientRunner.class);
    private static final String PROCESS_NAME = ManagementFactory.getRuntimeMXBean().getName();
    private static String connectionString;
    private static int clientRunTime;

    /**
     * Main entry point to client runner test application.
     *
     * @param args the command line arguments
     */
    public static void main(final String[] args) {

        boolean hasExceptions = false;
        if (args.length < 1) {
            LOGGER.error("Unexpected number of arguments. Required: connectionString [maxNumberOfClients]");
            System.exit(-1);
        }

        connectionString = args[0];
        final int maxNumberOfClients = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        clientRunTime = args.length > 2 ? Integer.parseInt(args[2]) : 30;

        final List<Map.Entry<ClientConnectionRunner, Thread>> runners = new ArrayList<>();

        try {
            for (int index = 0; index < maxNumberOfClients; index++) {
                startConnectionRunner(runners, index);
            }
        } catch (Exception e) {
            hasExceptions = true;
            writeException(e);
        } finally {
            for (Map.Entry<ClientConnectionRunner, Thread> entry : runners) {
                try {
                    waitForConnectionRunner(entry);
                } catch (Exception e) {
                    hasExceptions = true;
                    writeException(e);
                }
            }
            runners.clear();
        }
        System.exit(hasExceptions ? 1 : 0);
    }

    private static void waitForConnectionRunner(final Map.Entry<ClientConnectionRunner, Thread> entry) throws InterruptedException {
        LOGGER.info(PROCESS_NAME + ": Stopping entry");
        entry.getValue().join();
        if (entry.getKey().exception != null) {
            LOGGER.error("Connection failed", entry.getKey().exception);
        }
        LOGGER.info(PROCESS_NAME + ": Stopped entry");
    }

    private static void startConnectionRunner(
            final List<Map.Entry<ClientConnectionRunner, Thread>> runners,
            final int index) {
        LOGGER.info(PROCESS_NAME + ": Starting client " + index);
        final Thread runnerThread = getRunnerThread(runners);
        runnerThread.start();
        LOGGER.info(PROCESS_NAME + ": Started client " + index);
    }

    private static Thread getRunnerThread(
            final List<Map.Entry<ClientConnectionRunner, Thread>> runners) {
        final ClientConnectionRunner runner = new ClientConnectionRunner(connectionString, clientRunTime);
        final Thread runnerThread = new Thread(runner);
        runners.add(new AbstractMap.SimpleImmutableEntry<>(runner, runnerThread));
        return runnerThread;
    }

    private static void writeException(final Exception e) {
        LOGGER.error(PROCESS_NAME + ": Exception: " + e);
        LOGGER.error(Arrays.stream(e.getStackTrace())
                .map(StackTraceElement::toString)
                .collect(Collectors.joining(System.lineSeparator())));
    }

    private static class ClientConnectionRunner implements Runnable, AutoCloseable {
        public static final SecureRandom RANDOM = new SecureRandom();
        private volatile Exception exception = null;
        private final String connectionString;
        private final int waitTimeoutSECS;

        public ClientConnectionRunner(final String connectionString, final int waitTimeoutSECS) {
            this.connectionString = connectionString;
            this.waitTimeoutSECS = waitTimeoutSECS;
        }

        @Override
        public void run() {
            try (Connection connection = DriverManager.getConnection(connectionString)) {
                final boolean connected = connection.isValid(0);
                LOGGER.info("Connection is valid: " + connected);
                assert connected;
                final int randomExtension = RANDOM.nextInt(Math.max(1, (int) (0.25 * waitTimeoutSECS)));
                TimeUnit.SECONDS.sleep(waitTimeoutSECS + randomExtension);
            } catch (Exception e) {
                exception = e;
            }
        }

        @Override
        public void close() throws Exception {
        }

        public Exception getException() {
            return exception;
        }
    }
}
