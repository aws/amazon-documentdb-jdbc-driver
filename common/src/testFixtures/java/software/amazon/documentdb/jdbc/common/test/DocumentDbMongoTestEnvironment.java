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

package software.amazon.documentdb.jdbc.common.test;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongoShellStarter;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.Defaults;
import de.flapdoodle.embed.mongo.config.ImmutableMongoShellConfig;
import de.flapdoodle.embed.mongo.config.ImmutableMongodConfig.Builder;
import de.flapdoodle.embed.mongo.config.MongoCmdOptions;
import de.flapdoodle.embed.mongo.config.MongoShellConfig;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version.Main;
import de.flapdoodle.embed.process.config.RuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.LogWatchStreamProcessor;
import de.flapdoodle.embed.process.io.NamedOutputStreamProcessor;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.io.StreamProcessor;
import de.flapdoodle.embed.process.runtime.Network;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static de.flapdoodle.embed.process.io.Processors.namedConsole;
import static org.apache.logging.log4j.core.util.Assert.isEmpty;

public class DocumentDbMongoTestEnvironment extends DocumentDbAbstractTestEnvironment {
    private static final long INIT_TIMEOUT_MS = 30000;
    private static final String USER_ADDED_TOKEN = "Successfully added user";
    private static final String INTEGRATION_DATABASE_NAME = "integration";
    private static final String MONGO_LOCAL_HOST = "localhost";
    private static final String MONGO_USERNAME = "mongo";
    private static final boolean USE_AUTHENTICATION_DEFAULT = true;
    private static final String ADMIN_USERNAME = "admin";
    private static final String ADMIN_PASSWORD = "admin";
    private final boolean enableAuthentication;
    private final String databaseName;
    private MongodConfig mongoConfig = null;
    private MongodExecutable mongoExecutable = null;
    private MongodProcess mongoProcess = null;
    private int port = -1;

    /**
     * Creates a new {@link DocumentDbMongoTestEnvironment} with defaults.
     */
    DocumentDbMongoTestEnvironment() {
        this(INTEGRATION_DATABASE_NAME,
                MONGO_USERNAME,
                getRandomPassword(),
                USE_AUTHENTICATION_DEFAULT);
    }

    /**
     * Creates a new {@link DocumentDbMongoTestEnvironment}.
     *
     * @param databaseName the name of the database the user should have access to.
     * @param username the user name.
     * @param password the password for the user.
     * @param enableAuthentication indicator of whether to enable authentication.
     */
    DocumentDbMongoTestEnvironment(
            final String databaseName,
            final String username,
            final String password,
            final boolean enableAuthentication) {
        super(MONGO_LOCAL_HOST, username, password, "tls=false");
        this.enableAuthentication = enableAuthentication;
        this.databaseName = databaseName;
    }

    @Override
    protected boolean startEnvironment() throws Exception {
        if (isStarted() || mongoProcess != null) {
            return false;
        }

        port = Network.getFreeServerPort();
        final MongoCmdOptions cmdOptions = MongoCmdOptions.builder()
                .auth(enableAuthentication)
                .build();
        final MongodStarter starter = MongodStarter.getDefaultInstance();
        final Builder builder = MongodConfig.builder()
                .version(Main.V4_0)
                .net(new Net(port, Network.localhostIsIPv6()))
                .cmdOptions(cmdOptions);

        mongoConfig = builder.build();
        mongoExecutable = starter.prepare(mongoConfig);
        mongoProcess = mongoExecutable.start();
        addAdmin();
        createUser(databaseName, getUsername(), getPassword());
        return true;
    }

    @Override
    protected boolean stopEnvironment() {
        if (!isStarted() || mongoExecutable == null) {
            return false;
        }

        mongoProcess.stop();
        mongoExecutable.stop();
        mongoExecutable = null;
        mongoProcess = null;
        port = -1;
        return true;
    }

    @Override
    protected int getPort() {
        return port;
    }

    @Override
    protected boolean isBsonTypeCompatible(final BsonType bsonType) {
        // All BsonType are compatible with Mongo server.
        return true;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        return DocumentDbMongoTestEnvironment.class.getSimpleName() + "{" +
                " databaseName='" + databaseName + '\'' +
                ", username='" + getUsername() + '\'' +
                ", port=" + port +
                ", enableAuthentication=" + enableAuthentication +
                " }";
    }

    /**
     * Creates a user to the admin database with dbOwner role on another database.
     * @param databaseName the name of database to grant access for the user.
     * @param username the user name to create.
     * @param password the password for the user.
     * @throws IOException if unable to start the mongo shell process.
     */
    private void createUser(
            final String databaseName,
            final String username,
            final String password) throws IOException {

        final String[] roles = new String[] { "{\"db\":\"" + databaseName + "\",\"role\":\"dbOwner\"}" };
        final String scriptText = StringUtils.join(String.format("db = db.getSiblingDB('%s'); " +
                        "db.createUser({\"user\":\"%s\",\"pwd\":\"%s\",\"roles\":[%s]});%n" +
                        "db.getUser('%s');",
                ADMIN_DATABASE, username, password, StringUtils.join(roles, ","), username), "");
        runScriptAndWait(
                scriptText,
                new String[]{"already exists", "failed to load", "login failed"},
                ADMIN_USERNAME, ADMIN_PASSWORD);
    }

    private void addAdmin() throws IOException {
        final String scriptText = StringUtils.join(
                String.format("db.createUser(" +
                                "{\"user\":\"%s\",\"pwd\":\"%s\"," +
                                "\"roles\":[" +
                                "\"root\"," +
                                "{\"role\":\"userAdmin\",\"db\":\"admin\"}," +
                                "{\"role\":\"dbAdmin\",\"db\":\"admin\"}," +
                                "{\"role\":\"userAdminAnyDatabase\",\"db\":\"admin\"}," +
                                "{\"role\":\"dbAdminAnyDatabase\",\"db\":\"admin\"}," +
                                "{\"role\":\"clusterAdmin\",\"db\":\"admin\"}," +
                                "{\"role\":\"dbOwner\",\"db\":\"admin\"}," +
                                "]});%n",
                        ADMIN_USERNAME, ADMIN_PASSWORD));
        runScriptAndWait(scriptText,
                new String[]{"couldn't add user", "failed to load", "login failed"}, null, null);
    }

    private void runScriptAndWait(
            final String scriptText,
            final String[] failures,
            final String username,
            final String password) throws IOException {

        final StreamProcessor mongoOutput;
        if (!isEmpty(DocumentDbMongoTestEnvironment.USER_ADDED_TOKEN)) {
            mongoOutput = new MongoLogWatchStreamProcessor(
                    DocumentDbMongoTestEnvironment.USER_ADDED_TOKEN,
                    (failures != null)
                            ? new HashSet<>(Arrays.asList(failures))
                            : Collections.emptySet(),
                    namedConsole("[mongo shell output]"));
        } else {
            mongoOutput = new NamedOutputStreamProcessor("[mongo shell output]", Processors.console());
        }
        final RuntimeConfig runtimeConfig = Defaults.runtimeConfigFor(Command.Mongo)
                .processOutput(new ProcessOutput(
                        mongoOutput,
                        namedConsole("[mongo shell error]"),
                        Processors.console()))
                .build();
        final MongoShellStarter starter = MongoShellStarter.getInstance(runtimeConfig);
        final File scriptFile = writeTmpScriptFile(scriptText);
        final ImmutableMongoShellConfig.Builder builder = MongoShellConfig.builder();
        if (!isEmpty(DocumentDbAbstractTestEnvironment.ADMIN_DATABASE)) {
            builder.dbName(DocumentDbAbstractTestEnvironment.ADMIN_DATABASE);
        }
        if (!isEmpty(username)) {
            builder.userName(username);
        }
        if (!isEmpty(password)) {
            builder.password(password);
        }
        starter.prepare(builder
                .scriptName(scriptFile.getAbsolutePath())
                .version(mongoConfig.version())
                .net(mongoConfig.net())
                .build()).start();
        if (mongoOutput instanceof MongoLogWatchStreamProcessor) {
            ((MongoLogWatchStreamProcessor) mongoOutput).waitForResult(INIT_TIMEOUT_MS);
        }
    }

    private static File writeTmpScriptFile(final String scriptText) throws IOException {
        final File scriptFile = File.createTempFile("tempFile", ".js");
        scriptFile.deleteOnExit();
        final Writer writer = new OutputStreamWriter(new FileOutputStream(scriptFile),
                StandardCharsets.UTF_8);
        final BufferedWriter bw = new BufferedWriter(writer);
        bw.write(scriptText);
        bw.close();
        return scriptFile;
    }

    private static String getRandomPassword() {
        return UUID.randomUUID().toString().replaceAll("[-]", "");
    }

    /**
     * Watches the mongo or mongod output stream.
     */
    private static class MongoLogWatchStreamProcessor extends LogWatchStreamProcessor {
        private final Object mutex = new Object();
        private final String success;
        private final Set<String> failures;
        private volatile boolean found = false;

        /**
         * Creates a new MongoLogWatchStreamProcessor
         * @param success the string token to watch for to indicate success.
         * @param failures the set of strings to watch for to indicate failure.
         * @param destination the stream processor.
         */
        public MongoLogWatchStreamProcessor(
                final String success,
                final Set<String> failures,
                final StreamProcessor destination) {

            super(success, failures, destination);
            this.success = success;
            this.failures = failures;
        }

        @Override
        public void process(final String block) {
            if (containsSuccess(block) || containsFailure(block)) {
                synchronized (mutex) {
                    found = true;
                    mutex.notifyAll();
                }
            } else {
                super.process(block);
            }
        }

        private boolean containsSuccess(final String block) {
            return block.contains(success);
        }

        private boolean containsFailure(final String block) {
            for (String failure : failures) {
                if (block.contains(failure)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Waits for result for a result up to as long as given timeout.
         * @param timeout the timeout when waiting for a result.
         */
        public void waitForResult(final long timeout) {
            synchronized (mutex) {
                try {
                    while (!found) {
                        mutex.wait(timeout);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
