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

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.types.Decimal128;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Assertions;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperty;
import software.amazon.documentdb.jdbc.DocumentDbMetadataScanMethod;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Provides some base implementation of the {@link DocumentDbTestEnvironment} interface.
 */
public abstract class DocumentDbAbstractTestEnvironment implements DocumentDbTestEnvironment {
    protected static final String ADMIN_DATABASE = "admin";
    private static final String JDBC_TEMPLATE = "jdbc:documentdb://%s%s:%s/%s%s";

    private final String host;
    private final String username;
    private final String password;
    private final String options;
    private final String restrictedUsername;
    private final List<Entry<String, String>> temporaryCollections;

    private boolean isStarted = false;

    /**
     * Constructs a new {@link DocumentDbAbstractTestEnvironment} object.
     * @param host the host the test environment connects to.
     * @param username the user name on the test host.
     * @param password the password for the user on the test host.
     * @param options the options (if any) for mongo driver connection.
     */
    protected DocumentDbAbstractTestEnvironment(
            final String host,
            final String username,
            final String password,
            final String restrictedUsername,
            @Nullable final String options) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.options = options;
        this.restrictedUsername = restrictedUsername;
        this.temporaryCollections = new ArrayList<>();
    }

    /**
     * Perform environment-specific initialization.
     *
     * @return true, if a new environment was started, false, if the environment was already started.
     */
    protected abstract boolean startEnvironment() throws Exception;

    /**
     * Performs environment-specific clean-up.
     *
     * @return true, if the environment was stopped, false, if the environment was already stopped.
     */
    protected abstract boolean stopEnvironment() throws Exception;

    /**
     * Gets the port number for the environment.
     *
     * @return the port number.
     */
    protected abstract int getPort();

    /**
     * Gets an indicator of whether the given type is compatible for this environment.
     *
     * @param bsonType the {@link BsonType} to check.
     * @return true, if the given type is compatible.
     */
    protected abstract boolean isBsonTypeCompatible(final BsonType bsonType);

    /**
     * Gets an indicator of whether the environment is started.
     *
     * @return true, if the environment is started, false, otherwise.
     */
    protected boolean isStarted() {
        return isStarted;
    }

    /**
     * Gets the user name.
     *
     * @return the user name.
     */
    protected String getUsername() {
        return username;
    }

    protected String getRestrictedUsername() {
        return restrictedUsername;
    }

    /**
     * Gets the password.
     *
     * @return the password.
     */
    protected String getPassword() {
        return password;
    }

    @Override
    public String getJdbcConnectionString() {
        return String.format(JDBC_TEMPLATE,
                getCredentials(), getHost(), getPort(), getDatabaseName(), getOptions());
    }

    @Override
    public String getRestrictedUserConnectionString() {
        final String jdbcTemplate = "jdbc:documentdb://%s%s:%s/%s%s";
        return String.format(jdbcTemplate,
                getCredentials(true), getHost(), getPort(), getDatabaseName(), getOptions());
    }

    @Override
    public abstract String getDatabaseName();

    @Override
    public boolean start() throws Exception {
        final boolean started = startEnvironment();
        isStarted = true;
        return started;
    }

    @Override
    public boolean stop() throws Exception {
        if (isStarted) {
            try (MongoClient client = createMongoClient()) {
                for (Entry<String, String> entry : temporaryCollections) {
                    final MongoDatabase database = client.getDatabase(entry.getKey());
                    final MongoCollection<BsonDocument> collection = database
                            .getCollection(entry.getValue(), BsonDocument.class);
                    try {
                        collection.drop();
                    } catch (MongoException e) {
                        if (e.getCode() != 13) {
                            throw e;
                            // Ignore 'Authorization failure
                        }

                    }
                }
                temporaryCollections.clear();
            }
        }
        final boolean stopped = stopEnvironment();
        isStarted = false;
        return stopped;
    }


    @Override
    public String newCollectionName(final boolean isTemporary) {
        final String collectionName = UUID.randomUUID().toString().replaceAll("[-]", "");
        if (isTemporary) {
            temporaryCollections.add(new SimpleEntry<>(getDatabaseName(), collectionName));
        }
        return collectionName;
    }

    @Override
    public MongoClient createMongoClient() throws SQLException {
        return MongoClients.create(DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(getJdbcConnectionString())
                .buildMongoClientSettings());
    }

    @Override
    public void prepareSimpleConsistentData(final MongoCollection<BsonDocument> collection,
            final int recordCount) {
        for (int count = 0; count < recordCount; count++) {
            // Types not supported in DocumentDB
            //BsonRegularExpression
            //BsonJavaScript
            //BsonJavaScriptWithScope
            //BsonDecimal128
            final long dateTime = Instant.parse("2020-01-01T00:00:00.00Z").toEpochMilli();
            final BsonDocument document = new BsonDocument()
                    .append("_id", new BsonObjectId())
                    .append("fieldDouble", new BsonDouble(Double.MAX_VALUE))
                    .append("fieldString", new BsonString("新年快乐"))
                    .append("fieldObjectId", new BsonObjectId())
                    .append("fieldBoolean", new BsonBoolean(true))
                    .append("fieldDate", new BsonDateTime(dateTime))
                    .append("fieldInt", new BsonInt32(Integer.MAX_VALUE))
                    .append("fieldLong", new BsonInt64(Long.MAX_VALUE))
                    .append("fieldMaxKey", new BsonMaxKey())
                    .append("fieldMinKey", new BsonMinKey())
                    .append("fieldNull", new BsonNull())
                    .append("fieldBinary", new BsonBinary(new byte[]{0, 1, 2}));
            if (isBsonTypeCompatible(BsonType.DECIMAL128)) {
                document.append("fieldDecimal128", new BsonDecimal128(Decimal128.POSITIVE_INFINITY));
            }

            final InsertOneResult result = collection.insertOne(document);
            Assertions.assertEquals(count + 1, collection.countDocuments());
            Assertions.assertEquals(document.getObjectId("_id"), result.getInsertedId());
        }

    }

    private String getHost() {
        return host;
    }

    private String getOptions() {
        return options != null
                ? options.startsWith("?") ? options : "?" + options
                : "";
    }

    private String getCredentials() {
        return getCredentials(false);
    }

    private String getCredentials(final boolean isRestrictedUser) {
        return username != null && password != null
                ? String.format("%s:%s@", encodeValue(isRestrictedUser ? restrictedUsername : username), encodeValue(password)) : "";
    }

    private static String encodeValue(final String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            return value;
        }
    }

    @Override
    public void insertBsonDocuments(final String collectionName, final BsonDocument[] documents)
            throws SQLException {
        try (MongoClient client = createMongoClient()) {
            final MongoDatabase database = client.getDatabase(getDatabaseName());
            final MongoCollection<BsonDocument> collection =
                    database.getCollection(collectionName, BsonDocument.class);
            for (int count = 0; count < documents.length; count++) {
                collection.insertOne(documents[count]);
                Assertions.assertEquals(count + 1, collection.countDocuments());
            }
        }
    }

    @Override
    public String getJdbcConnectionString(final DocumentDbMetadataScanMethod scanMethod) {
        final String optionsWithScanMethod =
                getOptions() != null && getOptions().startsWith("?")
                        ? getOptions()
                        + "&"
                        + DocumentDbConnectionProperty.METADATA_SCAN_METHOD
                        + "="
                        + scanMethod.getName()
                        : "?" + DocumentDbConnectionProperty.METADATA_SCAN_METHOD + scanMethod.getName();
        return String.format(
                JDBC_TEMPLATE,
                getCredentials(),
                getHost(),
                getPort(),
                getDatabaseName(),
                optionsWithScanMethod);
    }
}
