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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class DocumentDbTest {
    private static int mongoPort = -1;

    /**
     * Gets the port number of the mongod process is listening to.
     * @return if the process is running, returns the port the mongod process is listening to, -1 otherwise.
     */
    protected static int getMongoPort() {
        return mongoPort;
    }

    /**
     * Sets the current listening port.
     *
     * @param port the current Mongo server port.
     */
    protected static void setMongoPort(final int port) {
        mongoPort = port;
    }
    /**
     * Creates a new MongoClient instance using the current port.
     *
     * @return a new instance of MongoClient.
     */
    protected static MongoClient createMongoClient() {
        return createMongoClient(null, null, null);
    }

    /**
     * Creates a new MongoClient instance using the current port.
     *
     * @param database the authenticating database to authenticate
     * @param username the username to authenticate
     * @param password the password to authenticate
     * @return a new instance of MongoClient.
     */
    protected static MongoClient createMongoClient(
            final String database, final String username, final String password) {
        return createMongoClient(database, username, password, null);
    }

    /**
     * Creates a new MongoClient instance using the current port.
     *
     * @param database the authenticating database to authenticate
     * @param username the username to authenticate
     * @param password the password to authenticate
     * @param options any additional options to pass. Format '[?]option=value[&option=value[...]]'
     * @return a new instance of MongoClient.
     */
    protected static MongoClient createMongoClient(
            final String database, final String username, final String password, final String options) {

        final int port = getMongoPort();
        final String credentials = username != null && password != null
                ? String.format("%s:%s@", encodeValue(username), encodeValue(password)) : "";
        final String hostname = "localhost";
        final String authDatabase = database != null ? "/" + database : "/";
        final String optionsValue = options != null
                ? options.startsWith("?") ? options : "?" + options
                : "";
        return MongoClients.create(String.format("mongodb://%s%s:%s%s%s",
                credentials, hostname, port, authDatabase, optionsValue));
    }

    private static String encodeValue(final String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            return value;
        }
    }
}
