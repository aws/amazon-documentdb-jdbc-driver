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

package software.amazon.documentdb.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleExtension;
import software.amazon.documentdb.jdbc.common.test.DocumentDbFlapDoodleTest;
import java.sql.SQLException;

/**
 * Tests for the DocumentDbDataSource
 */
@ExtendWith(DocumentDbFlapDoodleExtension.class)
public class DocumentDbDataSourceTest extends DocumentDbFlapDoodleTest {
    private static final String HOSTNAME = "localhost";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "testDb";
    private DocumentDbDataSource dataSource;

    @BeforeAll
    void setup() {
        // Add 1 valid user so we can successfully authenticate.
        createUser(DATABASE, USERNAME, PASSWORD);
    }

    /**
     * Instantiates data source object for testing.
     */
    @BeforeEach
    public void initialize() {
        dataSource = new DocumentDbDataSource();
    }

    /**
     * Tests and validates with valid properties.
     *
     * @throws SQLException on invalid validation of properties.
     */
    @Test
    public void testValidProperties() throws SQLException {
        dataSource.setUser("username");
        dataSource.setPassword("password");
        dataSource.setDatabase("testDb");
        dataSource.setHostname("host");
        dataSource.setReplicaSet("rs0");
        dataSource.setReadPreference(DocumentDbReadPreference.PRIMARY);
        dataSource.setApplicationName("appName");
        dataSource.setTlsEnabled(false);
        dataSource.setTlsAllowInvalidHostnames(false);
        dataSource.setLoginTimeout(5);
        dataSource.setRetryReadsEnabled(false);
        dataSource.validateRequiredProperties(); // Will throw SQL exception if invalid
        Assertions.assertEquals("username", dataSource.getUser());
        Assertions.assertEquals("password", dataSource.getPassword());
        Assertions.assertEquals("testDb", dataSource.getDatabase());
        Assertions.assertEquals("host", dataSource.getHostname());
        Assertions.assertEquals("rs0", dataSource.getReplicaSet());
        Assertions.assertEquals(DocumentDbReadPreference.PRIMARY, dataSource.getReadPreference());
        Assertions.assertEquals("appName", dataSource.getApplicationName());
        Assertions.assertFalse(dataSource.getTlsEnabled());
        Assertions.assertFalse(dataSource.getTlsAllowInvalidHosts());
        Assertions.assertEquals(5L, dataSource.getLoginTimeout());
        Assertions.assertFalse(dataSource.getRetryReadsEnabled());
    }

    /**
     * Tests invalid property settings.
     */
    @Test
    public void testInvalidPropertySettings()  {
        Assertions.assertDoesNotThrow(
                () -> dataSource.setReplicaSet("rs2"));
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.setLoginTimeout(-1));
    }

    /**
     * Tests required properties validation with invalid inputs.
     */
    @Test
    public void testMissingPropertiesValidation() {
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
        dataSource.setUser("");
        dataSource.setPassword("password");
        dataSource.setDatabase("db");
        dataSource.setHostname("validHost");
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
        dataSource.setUser("user");
        dataSource.setPassword(" ");
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
        dataSource.setPassword("password");
        dataSource.setDatabase("    ");
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
        dataSource.setDatabase("database");
        dataSource.setHostname("");
        Assertions.assertThrows(SQLException.class,
                () -> dataSource.validateRequiredProperties());
    }

    @Test
    void getConnection() throws SQLException {
        dataSource.setUser(USERNAME);
        dataSource.setPassword(PASSWORD);
        dataSource.setDatabase(DATABASE);
        dataSource.setHostname(HOSTNAME + ":" + getMongoPort());
        dataSource.setTlsEnabled(false);
        Assertions.assertNotNull(dataSource.getConnection());
    }

    @Test
    void getConnectionWithUsernamePassword() throws SQLException {
        dataSource.setDatabase(DATABASE);
        dataSource.setHostname(HOSTNAME + ":" + getMongoPort());
        dataSource.setTlsEnabled(false);
        Assertions.assertNotNull(dataSource.getConnection(USERNAME, PASSWORD));
    }

    @Test
    void getPooledConnection() throws SQLException {
        dataSource.setUser(USERNAME);
        dataSource.setPassword(PASSWORD);
        dataSource.setDatabase(DATABASE);
        dataSource.setHostname(HOSTNAME + ":" + getMongoPort());
        dataSource.setTlsEnabled(false);
        Assertions.assertNotNull(dataSource.getPooledConnection());
    }


    @Test
    void getPooledConnectionWithUsernamePassword() throws SQLException {
        dataSource.setDatabase(DATABASE);
        dataSource.setHostname(HOSTNAME + ":" + getMongoPort());
        dataSource.setTlsEnabled(false);
        Assertions.assertNotNull(dataSource.getPooledConnection(USERNAME, PASSWORD));
    }
}
