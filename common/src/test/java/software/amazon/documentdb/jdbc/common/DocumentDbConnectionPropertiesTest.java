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

package software.amazon.documentdb.jdbc.common;

import com.mongodb.MongoClientSettings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperties;
import software.amazon.documentdb.jdbc.DocumentDbConnectionProperty;
import software.amazon.documentdb.jdbc.DocumentDbMetadataScanMethod;
import software.amazon.documentdb.jdbc.DocumentDbReadPreference;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DocumentDbConnectionPropertiesTest {

    /**
     * Tests building the client settings and sanitized connection string from valid properties.
     */
    @Test
    @DisplayName("Tests building the client settings and sanitized connection string from valid properties.")
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only.")
    public void testValidProperties() {
         // Set properties.
         final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
         properties.setUser("USER");
         properties.setPassword("PASSWORD");
         properties.setDatabase("DATABASE");
         properties.setApplicationName("APPNAME");
         properties.setHostname("HOSTNAME");
         properties.setReplicaSet("rs0");
         properties.setLoginTimeout("100");
         properties.setMetadataScanLimit("100");
         properties.setTlsAllowInvalidHostnames("true");
         properties.setTlsEnabled("true");
         properties.setRetryReadsEnabled("true");

         // Get properties.
         Assertions.assertEquals("USER", properties.getUser());
         Assertions.assertEquals("PASSWORD", properties.getPassword());
         Assertions.assertEquals("DATABASE", properties.getDatabase());
         Assertions.assertEquals("APPNAME", properties.getApplicationName());
         Assertions.assertEquals("HOSTNAME", properties.getHostname());
         Assertions.assertEquals("rs0", properties.getReplicaSet());
         Assertions.assertEquals(100, properties.getLoginTimeout());
         Assertions.assertEquals(100, properties.getMetadataScanLimit());
         Assertions.assertTrue(properties.getTlsEnabled());
         Assertions.assertTrue(properties.getTlsAllowInvalidHostnames());
         Assertions.assertTrue(properties.getRetryReadsEnabled());

         // Build sanitized connection string.
         Assertions.assertEquals(
                 "//USER@HOSTNAME/DATABASE?appName=APPNAME&loginTimeoutSec=100&scanLimit=100&replicaSet=rs0&tlsAllowInvalidHostnames=true",
                 properties.buildSanitizedConnectionString());

         // Build client settings.
         final MongoClientSettings settings = properties.buildMongoClientSettings();
         Assertions.assertNotNull(settings);
         Assertions.assertEquals("USER", settings.getCredential().getUserName());
         Assertions.assertEquals("PASSWORD", String.valueOf(settings.getCredential().getPassword()));
         Assertions.assertEquals("hostname", settings.getClusterSettings().getHosts().get(0).getHost());
         Assertions.assertEquals("APPNAME", settings.getApplicationName());
         Assertions.assertEquals("rs0", settings.getClusterSettings().getRequiredReplicaSetName());
         Assertions.assertEquals(100, settings.getSocketSettings().getConnectTimeout(TimeUnit.SECONDS));
         Assertions.assertTrue(settings.getRetryReads());
         Assertions.assertTrue(settings.getSslSettings().isEnabled());
         Assertions.assertTrue(settings.getSslSettings().isInvalidHostNameAllowed());
    }

    /**
     * Tests setting the scan method with the DocumentDbScanMethod enum.
     */
    @Test
    @DisplayName("Tests setting the scan method with the DocumentDbScanMethod enum.")
    public void testMetadataScanMethods() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
        properties.setMetadataScanMethod("random");
        Assertions.assertEquals(DocumentDbMetadataScanMethod.RANDOM, properties.getMetadataScanMethod());
        properties.setMetadataScanMethod("all");
        Assertions.assertEquals(DocumentDbMetadataScanMethod.ALL, properties.getMetadataScanMethod());
        properties.setMetadataScanMethod("idForward");
        Assertions.assertEquals(DocumentDbMetadataScanMethod.ID_FORWARD, properties.getMetadataScanMethod());
        properties.setMetadataScanMethod("idReverse");
        Assertions.assertEquals(DocumentDbMetadataScanMethod.ID_REVERSE, properties.getMetadataScanMethod());
        properties.setMetadataScanMethod("garbage");
        Assertions.assertNull(properties.getMetadataScanMethod());
    }

    /**
     * Tests setting the read preference with the DocumentDbReadPreference enum.
     */
    @Test
    @DisplayName("Tests setting the read preference with the DocumentDbReadPreference enum.")
    public void testReadPreferences() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
        properties.setReadPreference("primary");
        Assertions.assertEquals(DocumentDbReadPreference.PRIMARY, properties.getReadPreference());
        properties.setReadPreference("primaryPreferred");
        Assertions.assertEquals(DocumentDbReadPreference.PRIMARY_PREFERRED, properties.getReadPreference());
        properties.setReadPreference("secondary");
        Assertions.assertEquals(DocumentDbReadPreference.SECONDARY, properties.getReadPreference());
        properties.setReadPreference("secondaryPreferred");
        Assertions.assertEquals(DocumentDbReadPreference.SECONDARY_PREFERRED, properties.getReadPreference());
        properties.setReadPreference("nearest");
        Assertions.assertEquals(DocumentDbReadPreference.NEAREST, properties.getReadPreference());
        properties.setReadPreference("garbage");
        Assertions.assertNull(properties.getReadPreference());
    }

    /**
     * Tests the properties builder function.
     */
    @Test
    @DisplayName("Tests the properties builder function on various connection strings.")
    public void testSetPropertiesFromConnectionString() throws SQLException {
        final Properties info = new Properties();
        info.clear();
        String connectionString = "jdbc:documentdb://username:password@localhost/database";
        DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, "jdbc:documentdb:");
        Assertions.assertEquals(4, properties.size());
        Assertions.assertEquals("localhost", properties.getProperty("host"));
        Assertions.assertEquals("database", properties.getProperty("database"));
        Assertions.assertEquals("username", properties.getProperty("user"));
        Assertions.assertEquals("password", properties.getProperty("password"));

        // Connection string does not override existing properties.
        connectionString = "jdbc:documentdb://username:password@127.0.0.1/newdatabase";
        properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, "jdbc:documentdb:");
        Assertions.assertEquals(4, properties.size());
        Assertions.assertEquals("127.0.0.1", properties.getProperty("host"));
        Assertions.assertEquals("newdatabase", properties.getProperty("database"));
        Assertions.assertEquals("username", properties.getProperty("user"));
        Assertions.assertEquals("password", properties.getProperty("password"));

        // Get user (unencoded) name and password.
        info.clear();
        connectionString = "jdbc:documentdb://user%20name:pass%20word@127.0.0.1/newdatabase";
        properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, "jdbc:documentdb:");
        Assertions.assertEquals(4, properties.size());
        Assertions.assertEquals("127.0.0.1", properties.getProperty("host"));
        Assertions.assertEquals("newdatabase", properties.getProperty("database"));
        Assertions.assertEquals("user name", properties.getProperty("user"));
        Assertions.assertEquals("pass word", properties.getProperty("password"));

        // Check that all properties can be added.
        info.clear();
        connectionString = "jdbc:documentdb://user%20name:pass%20word@127.0.0.1/newdatabase" +
                "?" + DocumentDbConnectionProperty.READ_PREFERENCE.getName() + "=" + "secondaryPreferred" +
                "&" + DocumentDbConnectionProperty.APPLICATION_NAME.getName() + "=" + "application" +
                "&" + DocumentDbConnectionProperty.REPLICA_SET.getName() + "=" + "rs0" +
                "&" + DocumentDbConnectionProperty.TLS_ENABLED.getName() + "=" + "true" +
                "&" + DocumentDbConnectionProperty.TLS_ALLOW_INVALID_HOSTNAMES.getName() + "=" + "true" +
                "&" + DocumentDbConnectionProperty.LOGIN_TIMEOUT_SEC.getName() + "=" + "4" +
                "&" + DocumentDbConnectionProperty.RETRY_READS_ENABLED.getName() + "=" + "true" +
                "&" + DocumentDbConnectionProperty.METADATA_SCAN_METHOD.getName() + "=" + "random" +
                "&" + DocumentDbConnectionProperty.METADATA_SCAN_LIMIT.getName() + "=" + "1";
        properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, "jdbc:documentdb:");
        Assertions.assertEquals(DocumentDbConnectionProperty.values().length, properties.size());

        // Check that unsupported properties are ignored.
        connectionString = "jdbc:documentdb://user%20name:pass%20word@127.0.0.1/newdatabase" +
                "?" + "maxStalenessSeconds" + "=" + "value";
        properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, "jdbc:documentdb:");
        Assertions.assertEquals(4, properties.size());
        Assertions.assertNull(properties.getProperty("maxStalenessSeconds"));
    }

}
