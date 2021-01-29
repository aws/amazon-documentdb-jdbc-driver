/*
 * Copyright <2021> Amazon.com, final Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, final Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, final WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, final either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
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
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Assertions;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static de.flapdoodle.embed.process.io.Processors.namedConsole;
import static org.apache.logging.log4j.core.util.Assert.isEmpty;

/**
 * Base class for DocumentDb tests
 */
public class DocumentDbTest {

    protected static final int DEFAULT_PORT = 27017;
    private static final long INIT_TIMEOUT_MS = 30000;
    private static final String USER_ADDED_TOKEN = "Successfully added user";
    private static MongodConfig mongoConfig;
    private static MongodExecutable mongoExecutable = null;
    private static MongodProcess mongoProcess = null;
    private static int mongoPort = -1;
    private static final String ADMIN_DATABASE = "admin";
    private static final String ADMIN_USERNAME = "admin";
    private static final String ADMIN_PASSWORD = "admin";

    /**
     * Starts the mongod using default parameters.
     * @return returns true if the mongod is started, or false if already started.
     * @throws IOException if unable to start the mongod.
     */
    protected static boolean startMongoDbInstance() throws IOException {
        return startMongoDbInstance(Network.getFreeServerPort());
    }

    /**
     * Starts the mongod using custom port number.
     * @param port the port number that mongod listens on.
     * @return returns true if the mongod is started, or false if already started.
     * @throws IOException if unable to start the mongod.
     */
    protected static boolean startMongoDbInstance(final int port) throws IOException {
        return startMongoDbInstance(port, false);
    }

    /**
     * Starts the mongod using custom command options.
     * @param enableAuthentication indicates whether to start the process with authentication enabled.
     * @return returns true if the mongod is started, or false if already started.
     * @throws IOException if unable to start the mongod.
     */
    protected static boolean startMongoDbInstance(final boolean enableAuthentication) throws IOException {
        return startMongoDbInstance(Network.getFreeServerPort(), enableAuthentication);
    }

    /**
     * Starts the mongod using custom port and command options.
     * @param port the port number that mongod listens on.
     * @param enableAuthentication indicates whether to start the process with authentication enabled.
     * @return returns true if the mongod is started, or false if already started.
     * @throws IOException if unable to start the mongod.
     */
    protected static synchronized boolean startMongoDbInstance(
            final int port,
            final boolean enableAuthentication) throws IOException {

        if (mongoExecutable != null) {
            return false;
        }

        final MongoCmdOptions cmdOptions = MongoCmdOptions.builder()
                .auth(enableAuthentication)
                .build();
        mongoPort = port;
        final MongodStarter starter = MongodStarter.getDefaultInstance();
        final Builder builder = MongodConfig.builder()
                .version(Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()));
        if (cmdOptions != null) {
            builder.cmdOptions(cmdOptions);
        }

        mongoConfig = builder.build();
        mongoExecutable = starter.prepare(mongoConfig);
        mongoProcess = mongoExecutable.start();
        addAdmin();

        return true;
    }

    /**
     * Stops the running mongod process.
     * @return returns true if the mongod is stopped, or false if already stopped.
     */
    protected static synchronized boolean stopMongoDbInstance() {
        if (mongoExecutable == null) {
            return false;
        }

        mongoProcess.stop();
        mongoExecutable.stop();
        mongoExecutable = null;
        mongoProcess = null;
        mongoPort = -1;
        return true;
    }

    /**
     * Gets whether the mongod process is running.
     * @return returns true if process is running, false otherwise.
     */
    protected static synchronized boolean isMongoDbProcessRunning() {
        return mongoProcess != null && mongoProcess.isProcessRunning();
    }

    /**
     * Gets the port number of the mongod process is listening to.
     * @return if the process is running, returns the port the mongod process is listening to, -1 otherwise.
     */
    protected static synchronized int getMongoPort() {
        return mongoProcess != null ? mongoProcess.getConfig().net().getPort() : DEFAULT_PORT;
    }

    /**
     * Creates a new MongoClient instance using the current port.
     * @return a new instance of MongoClient.
     */
    protected static MongoClient createMongoClient() {
        return createMongoClient(null, null, null);
    }

    /**
     * Creates a new MongoClient instance using the current port.
     * @param database the authenticating database to authenticate
     * @param username the username to authenticate
     * @param password the password to authenticate
     * @return a new instance of MongoClient.
     */
    protected static MongoClient createMongoClient(
            final String database, final String username, final String password) {

        final int port = getMongoPort();
        final String credentials = username != null && password != null
                ? String.format("%s:%s@", username, password) : "";
        final String hostname = "localhost";
        final String authDatabase = database != null ? "/" + database : "";
        return MongoClients.create(String.format("mongodb://%s%s:%s%s",
                credentials, hostname, port, authDatabase));
    }

    /**
     * Creates a user to the admin database with dbOwner role on another database.
     * @param databaseName the name of database to grant access for the user.
     * @param username the user name to create.
     * @param password the password for the user.
     * @throws IOException if unable to start the mongo shell process.
     */
    protected static void createUser(
            final String databaseName,
            final String username,
            final String password) throws IOException {

        final String[] roles = new String[] { "{\"db\":\"" + databaseName + "\",\"role\":\"dbOwner\"}" };
        final String scriptText = StringUtils.join(String.format("db = db.getSiblingDB('%s'); " +
                        "db.createUser({\"user\":\"%s\",\"pwd\":\"%s\",\"roles\":[%s]});\n" +
                        "db.getUser('%s');",
                ADMIN_DATABASE, username, password, StringUtils.join(roles, ","), username), "");
        runScriptAndWait(
                scriptText,
                USER_ADDED_TOKEN,
                new String[]{"already exists", "failed to load", "login failed"},
                ADMIN_DATABASE, ADMIN_USERNAME, ADMIN_PASSWORD);
    }

    /**
     * Prepares data for a given database and collection.
     * @param databaseName - the name of the database to insert data into.
     * @param collectionName - the name of the collection to insert data into.
     * @param recordCount - the number of records to insert data into.
     */
    protected static void prepareSimpleConsistentData(
            final String databaseName,
            final String collectionName,
            final int recordCount) {

        try (MongoClient client = createMongoClient()) {
            final MongoDatabase database = client.getDatabase(databaseName);
            final MongoCollection<BsonDocument> collection = database
                    .getCollection(collectionName, BsonDocument.class);

            for (int count = 0; count < recordCount; count++) {
                // Types not supported in DocumentDB
                //BsonRegularExpression
                //BsonJavaScript
                //BsonJavaScriptWithScope
                //BsonDecimal128
                final BsonDocument document = new BsonDocument()
                        .append("_id", new BsonObjectId())
                        .append("fieldDouble", new BsonDouble(Double.MAX_VALUE))
                        .append("fieldString", new BsonString("String value"))
                        .append("fieldObjectId", new BsonObjectId())
                        .append("fieldBoolean", new BsonBoolean(true))
                        .append("fieldDate", new BsonDateTime(
                                Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli()))
                        .append("fieldInt", new BsonInt32(Integer.MAX_VALUE))
                        .append("fieldTimeStamp", new BsonTimestamp(1, 2))
                        .append("fieldLong", new BsonInt64(Long.MAX_VALUE))
                        .append("fieldMaxKey", new BsonMaxKey())
                        .append("fieldMinKey", new BsonMinKey())
                        .append("fieldNull", new BsonNull());

                final InsertOneResult result = collection.insertOne(document);
                Assertions.assertEquals(count + 1, collection.countDocuments());
                Assertions.assertEquals(document.getObjectId("_id"), result.getInsertedId());
            }
        }
    }

    private static void addAdmin() throws IOException {
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
                                "]});\n",
                        ADMIN_USERNAME, ADMIN_PASSWORD));
        runScriptAndWait(scriptText, USER_ADDED_TOKEN, new String[]{"couldn't add user", "failed to load", "login failed"}, "admin", null, null);
    }

    private static void runScriptAndWait(
            final String scriptText,
            final String token,
            final String[] failures,
            final String dbName,
            final String username,
            final String password) throws IOException {

        final StreamProcessor mongoOutput;
        if (!isEmpty(token)) {
            mongoOutput = new MongoLogWatchStreamProcessor(
                    token,
                    (failures != null)
                            ? new HashSet<>(Arrays.asList(failures))
                            : Collections.<String>emptySet(),
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
        if (!isEmpty(dbName)) {
            builder.dbName(dbName);
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
        final File scriptFile = File.createTempFile("tempfile", ".js");
        scriptFile.deleteOnExit();
        final BufferedWriter bw = new BufferedWriter(new FileWriter(scriptFile));
        bw.write(scriptText);
        bw.close();
        return scriptFile;
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
