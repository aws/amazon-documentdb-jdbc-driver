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

package software.amazon.documentdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Tests for the DocumentDbDriver
 */
public class DocumentDbDriverTest extends DocumentDBTest {

    /**
     * Initializes the test class.
     * @throws SQLException if a driver manager error occurs.
     */
    @BeforeAll
    public static void initialize() throws SQLException {
        // Clear out any other registered drivers.
        final Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            final Driver driver = drivers.nextElement();
            DriverManager.deregisterDriver(driver);
        }
        // Ensure our driver is registered.
        DriverManager.registerDriver(new DocumentDbDriver());
    }

    /**
     * Test for valid supported connection strings.
     */
    @Test
    public void testValidConnectionString() throws SQLException {
        //TODO : Fix the commented out tests.
        final String[] tests = new String[] {
                "jdbc:documentdb://user:password@localhost/database",
                //"jdbc:documentdb://user:password@localhost:65535/database",
                "jdbc:documentdb://user:password@127.0.0.1/database",
                "jdbc:documentdb://user%20name:pass%20word@localhost/database",
                //"jdbc:documentdb://user%20name:pass%20word@localhost:1/database?ssl=true",
                //"jdbc:documentdb://user%20name:pass%20word@localhost:1/database?tls=true",
                //"jdbc:documentdb://user%20name:pass%20word@localhost:1/database?replicaSet=rs0",
        };

        // Add 2 valid users to the local MongoDB instance.
        addUser("database", "user", "password");
        addUser("database", "user name", "pass word");

        for (String test : tests) {
            Assertions.assertNotNull(DriverManager.getDriver(test));
            Assertions.assertNotNull(DriverManager.getConnection(test));
        }
    }

    /**
     * Test invalid connection strings.
     */
    @Test
    public void testInvalidConnectionString() {
        final Map<String, String> tests = new HashMap<String, String>() {{
            put("jdbx:documentdb://localhost/database", "No suitable driver");
            put("documentdb://localhost/database", "No suitable driver");
            put("jdbc:documentdbx://localhost/database", "No suitable driver");
            put("jdbc:mongodb://localhost/database", "No suitable driver");
        }};

        for (Entry<String, String> test : tests.entrySet()) {
            Assertions.assertEquals(test.getValue(),
                    Assertions.assertThrows(SQLException.class, () -> DriverManager.getDriver(test.getKey()))
                            .getMessage());
            Assertions.assertEquals(String.format("No suitable driver found for %s", test.getKey()),
                    Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(test.getKey()))
                            .getMessage());
        }
    }

    /**
     * Test null connection strings.
     */
    @Test
    public void testNullConnectionString() {
        final Map<String, String> tests = new HashMap<String, String>() {{
            put(null, "No suitable driver");
        }};

        for (Entry<String, String> test : tests.entrySet()) {
            Assertions.assertEquals(test.getValue(),
                    Assertions.assertThrows(SQLException.class, () -> DriverManager.getDriver(test.getKey()))
                            .getMessage());
            Assertions.assertEquals("The url cannot be null",
                    Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(test.getKey()))
                            .getMessage());
        }
    }

    /**
     * Test invalid connection strings that fail semantics check
     * @throws SQLException thrown when a driver or connection error is encountered.
     */
    @Test
    public void testInvalidMongoDbConnectionString() throws SQLException {
        final Map<String, String> tests = new HashMap<String, String>() {{
            put("jdbc:documentdb://localhost:1/database", "User and password are required to connect. Syntax: 'jdbc:documentdb://<user>:<password>@<hostname>/<database>[?options...]'");
            put("jdbc:documentdb://username:password@localhost:1:2/database", "The connection string contains an invalid host 'localhost:1:2'. Reserved characters such as ':' must be escaped according RFC 2396. Any IPv6 address literal must be enclosed in '[' and ']' according to RFC 2732.");
            put("jdbc:documentdb://username:password@localhost:1/", "Database is required to connect. Syntax: 'jdbc:documentdb://<user>:<password>@<hostname>/<database>[?options...]'");
            put("jdbc:documentdb://username:password@localhost:1,localhost:2/database", "Only one host is supported. Use the replicaSet option to connect to a replica set.");
        }};

        for (Entry<String, String> test : tests.entrySet()) {
            Assertions.assertNotNull(DriverManager.getDriver(test.getKey()));
            Assertions.assertEquals(test.getValue(),
                    Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection(test.getKey()))
                            .getMessage());
        }
    }

    /**
     * Tests the properties builder function
     */
    @Test
    public void testSetPropertiesFromConnectionString() throws SQLException {
        final Properties info = new Properties();

        info.clear();
        String connectionString = "mongodb://username:password@localhost/database";
        DocumentDbDriver.setPropertiesFromConnectionString(info, connectionString);
        Assertions.assertEquals(4, info.size());
        Assertions.assertEquals("localhost", info.getProperty("host"));
        Assertions.assertEquals("database", info.getProperty("database"));
        Assertions.assertEquals("username", info.getProperty("user"));
        Assertions.assertEquals("password", info.getProperty("password"));

        // Connection string does not override existing properties.
        connectionString = "mongodb://username:password@127.0.0.1/newdatabase";
        DocumentDbDriver.setPropertiesFromConnectionString(info, connectionString);
        Assertions.assertEquals(4, info.size());
        Assertions.assertEquals("localhost", info.getProperty("host"));
        Assertions.assertEquals("database", info.getProperty("database"));
        Assertions.assertEquals("username", info.getProperty("user"));
        Assertions.assertEquals("password", info.getProperty("password"));

        // Get user (unencoded) name and password.
        info.clear();
        connectionString = "mongodb://user%20name:pass%20word@127.0.0.1/newdatabase";
        DocumentDbDriver.setPropertiesFromConnectionString(info, connectionString);
        Assertions.assertEquals(4, info.size());
        Assertions.assertEquals("127.0.0.1", info.getProperty("host"));
        Assertions.assertEquals("newdatabase", info.getProperty("database"));
        Assertions.assertEquals("user name", info.getProperty("user"));
        Assertions.assertEquals("pass word", info.getProperty("password"));

        // Check that all properties can be added.
        info.clear();
        connectionString = "mongodb://user%20name:pass%20word@127.0.0.1/newdatabase" +
                "?" + DocumentDbConnectionProperty.READ_PREFERENCE.getName() + "=" + "secondaryPreferred" +
                "&" + DocumentDbConnectionProperty.APPLICATION_NAME.getName() + "=" + "application" +
                "&" + DocumentDbConnectionProperty.REPLICA_SET.getName() + "=" + "rs0" +
                "&" + DocumentDbConnectionProperty.SERVER_SELECTION_TIMEOUT_MS.getName() + "=" + "1" +
                "&" + DocumentDbConnectionProperty.LOCAL_THRESHOLD_MS.getName() + "=" + "2" +
                "&" + DocumentDbConnectionProperty.HEARTBEAT_FREQUENCY_MS.getName() + "=" + "3" +
                "&" + DocumentDbConnectionProperty.TLS_ENABLED.getName() + "=" + "true" +
                "&" + DocumentDbConnectionProperty.TLS_ALLOW_INVALID_HOSTNAMES.getName() + "=" + "true" +
                "&" + DocumentDbConnectionProperty.CONNECT_TIMEOUT_MS.getName() + "=" + "4" +
                "&" + DocumentDbConnectionProperty.SOCKET_TIMEOUT_MS.getName() + "=" + "5" +
                "&" + DocumentDbConnectionProperty.MAX_POOL_SIZE.getName() + "=" + "6" +
                "&" + DocumentDbConnectionProperty.MIN_POOL_SIZE.getName() + "=" + "1" +
                "&" + DocumentDbConnectionProperty.WAIT_QUEUE_TIMEOUT_MS.getName() + "=" + "7" +
                "&" + DocumentDbConnectionProperty.MAX_IDLE_TIME_MS.getName() + "=" + "8" +
                "&" + DocumentDbConnectionProperty.MAX_LIFE_TIME_MS.getName() + "=" + "9" +
                "&" + DocumentDbConnectionProperty.RETRY_READS_ENABLED.getName() + "=" + "true" +
                "&" + DocumentDbConnectionProperty.UUID_REPRESENTATION.getName() + "=" + "standard";
        DocumentDbDriver.setPropertiesFromConnectionString(info, connectionString);
        Assertions.assertEquals(DocumentDbConnectionProperty.values().length, info.size());
    }
}
