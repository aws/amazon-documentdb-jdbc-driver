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

import com.mongodb.MongoClientSettings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getClassPathLocationName;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getDocumentdbHomePathName;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getPath;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getSshPrivateKeyFileSearchPaths;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.getUserHomePathName;

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
        properties.setTlsCAFilePath("src/main/resources/rds-ca-2019-root.pem");
        properties.setSshUser("SSHUSER");
        properties.setSshHostname("SSHHOST");
        properties.setSshPrivateKeyFile("~/.ssh/test-file-name.pem");
        properties.setSshPrivateKeyPassphrase("PASSPHRASE");
        properties.setSshStrictHostKeyChecking("false");
        properties.setSshKnownHostsFile("~/.ssh/unknown_hosts");
        properties.setDefaultFetchSize("1000");
        properties.setRefreshSchema("true");

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
        Assertions.assertEquals("src/main/resources/rds-ca-2019-root.pem",
                properties.getTlsCAFilePath());
        Assertions.assertEquals("SSHUSER", properties.getSshUser());
        Assertions.assertEquals("SSHHOST", properties.getSshHostname());
        Assertions.assertEquals("~/.ssh/test-file-name.pem", properties.getSshPrivateKeyFile());
        Assertions.assertEquals("PASSPHRASE", properties.getSshPrivateKeyPassphrase());
        Assertions.assertFalse(properties.getSshStrictHostKeyChecking());
        Assertions.assertEquals("~/.ssh/unknown_hosts", properties.getSshKnownHostsFile());
        Assertions.assertEquals(1000, properties.getDefaultFetchSize());
        Assertions.assertTrue(properties.getRefreshSchema());

        // Build sanitized connection string.
        Assertions.assertEquals(
                "//USER@HOSTNAME/DATABASE?appName=APPNAME"
                        + "&loginTimeoutSec=100"
                        + "&scanLimit=100"
                        + "&replicaSet=rs0"
                        + "&tlsAllowInvalidHostnames=true"
                        + "&tlsCAFile=src/main/resources/rds-ca-2019-root.pem"
                        + "&sshUser=SSHUSER"
                        + "&sshHost=SSHHOST"
                        + "&sshPrivateKeyFile=~/.ssh/test-file-name.pem"
                        + "&sshStrictHostKeyChecking=false"
                        + "&sshKnownHostsFile=~/.ssh/unknown_hosts"
                        + "&defaultFetchSize=1000"
                        + "&refreshSchema=true",
                properties.buildSanitizedConnectionString());

        // Build client settings.
        final MongoClientSettings settings = properties.buildMongoClientSettings();
        Assertions.assertNotNull(settings);
        Assertions.assertEquals("USER", settings.getCredential().getUserName());
        Assertions.assertEquals("PASSWORD", String.valueOf(settings.getCredential().getPassword()));
        Assertions.assertEquals("hostname",
                settings.getClusterSettings().getHosts().get(0).getHost());
        Assertions.assertEquals("APPNAME", settings.getApplicationName());
        Assertions.assertEquals("rs0", settings.getClusterSettings().getRequiredReplicaSetName());
        Assertions.assertEquals(100,
                settings.getSocketSettings().getConnectTimeout(TimeUnit.SECONDS));
        Assertions.assertTrue(settings.getRetryReads());
        Assertions.assertTrue(settings.getSslSettings().isEnabled());
        Assertions.assertTrue(settings.getSslSettings().isInvalidHostNameAllowed());
        Assertions.assertNotNull(settings.getSslSettings().getContext().getClientSessionContext());
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
        String connectionString = "jdbc:documentdb://username:password@localhost/database";
        DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, DOCUMENT_DB_SCHEME);
        Assertions.assertEquals(4, properties.size());
        Assertions.assertEquals("localhost", properties.getProperty("host"));
        Assertions.assertEquals("database", properties.getProperty("database"));
        Assertions.assertEquals("username", properties.getProperty("user"));
        Assertions.assertEquals("password", properties.getProperty("password"));

        // Connection string does not override existing properties.
        connectionString = "jdbc:documentdb://username:password@127.0.0.1/newdatabase";
        properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, DOCUMENT_DB_SCHEME);
        Assertions.assertEquals(4, properties.size());
        Assertions.assertEquals("127.0.0.1", properties.getProperty("host"));
        Assertions.assertEquals("newdatabase", properties.getProperty("database"));
        Assertions.assertEquals("username", properties.getProperty("user"));
        Assertions.assertEquals("password", properties.getProperty("password"));

        // Get user (unencoded) name and password.
        info.clear();
        connectionString = "jdbc:documentdb://user%20name:pass%20word@127.0.0.1/newdatabase";
        properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, DOCUMENT_DB_SCHEME);
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
                "&" + DocumentDbConnectionProperty.TLS_CA_FILE.getName() + "=" + "~/rds-ca-2019-root.pem" +
                "&" + DocumentDbConnectionProperty.LOGIN_TIMEOUT_SEC.getName() + "=" + "4" +
                "&" + DocumentDbConnectionProperty.RETRY_READS_ENABLED.getName() + "=" + "true" +
                "&" + DocumentDbConnectionProperty.METADATA_SCAN_METHOD.getName() + "=" + "random" +
                "&" + DocumentDbConnectionProperty.METADATA_SCAN_LIMIT.getName() + "=" + "1" +
                "&" + DocumentDbConnectionProperty.SCHEMA_PERSISTENCE_STORE.getName() + "=" + "file" +
                "&" + DocumentDbConnectionProperty.SCHEMA_NAME.getName() + "=" + "notDefault" +
                "&" + DocumentDbConnectionProperty.SSH_USER.getName() + "=" + "sshUser" +
                "&" + DocumentDbConnectionProperty.SSH_HOSTNAME.getName() + "=" + "sshHost" +
                "&" + DocumentDbConnectionProperty.SSH_PRIVATE_KEY_FILE.getName() + "=" + "~/.ssh/key.pem" +
                "&" + DocumentDbConnectionProperty.SSH_PRIVATE_KEY_PASSPHRASE.getName() + "=" + "passphrase" +
                "&" + DocumentDbConnectionProperty.SSH_STRICT_HOST_KEY_CHECKING.getName() + "=" + "false" +
                "&" + DocumentDbConnectionProperty.SSH_KNOWN_HOSTS_FILE.getName() + "=" + "~/.ssh/known_hosts" +
                "&" + DocumentDbConnectionProperty.DEFAULT_FETCH_SIZE.getName() + "=" + "1000" +
                "&" + DocumentDbConnectionProperty.REFRESH_SCHEMA.getName() + "=" + "true";
        properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, DOCUMENT_DB_SCHEME);
        Assertions.assertEquals(DocumentDbConnectionProperty.values().length, properties.size());

        // Check that unsupported properties are ignored.
        connectionString = "jdbc:documentdb://user%20name:pass%20word@127.0.0.1/newdatabase" +
                "?" + "maxStalenessSeconds" + "=" + "value";
        properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, DOCUMENT_DB_SCHEME);
        Assertions.assertEquals(4, properties.size());
        Assertions.assertNull(properties.getProperty("maxStalenessSeconds"));
    }

    @DisplayName("Test that non-existent tlsCAfile is handled correctly.")
    @SuppressFBWarnings(value = "HARD_CODE_PASSWORD", justification = "Hardcoded for test purposes only.")
    @Test
    void testInvalidTlsCAFilePath() {
        final DocumentDbConnectionProperties properties1 = new DocumentDbConnectionProperties();
        properties1.setUser("USER");
        properties1.setPassword("PASSWORD");
        properties1.setDatabase("DATABASE");
        properties1.setHostname("HOSTNAME");
        properties1.setTlsEnabled("true");
        properties1.setTlsCAFilePath("~/invalid-filename.pem");
        final String pattern = Matcher.quoteReplacement("TLS Certificate Authority file '")
                + ".*" + Matcher.quoteReplacement("invalid-filename.pem' not found.");
        Assertions.assertTrue(
                Assertions.assertThrows(SQLException.class, properties1::buildMongoClientSettings)
                .getMessage().matches(pattern));

        final DocumentDbConnectionProperties properties2 = new DocumentDbConnectionProperties();
        properties2.setUser("USER");
        properties2.setPassword("PASSWORD");
        properties2.setDatabase("DATABASE");
        properties2.setHostname("HOSTNAME");
        // tlsCAFile option is ignored if tls is false.
        properties2.setTlsEnabled("false");
        properties2.setTlsCAFilePath("~/invalid-filename.pem");
        Assertions.assertDoesNotThrow(
                (ThrowingSupplier<MongoClientSettings>) properties2::buildMongoClientSettings);
    }

    @Test()
    @DisplayName("Tests the getPath method.")
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
            final Path path3 = getPath(tempFilename1, getSshPrivateKeyFileSearchPaths());
            Assertions.assertEquals(Paths.get(getUserHomePathName(), tempFilename1), path3);
        } finally {
            Assertions.assertTrue(homeTempFilePath != null && homeTempFilePath.toFile().delete());
        }

        // Test that it will use the .documentdb folder under the user's home path
        Path documentDbTempFilePath = null;
        try {
            documentDbTempFilePath = Paths.get(getDocumentdbHomePathName(), tempFilename1);
            Assertions.assertTrue(documentDbTempFilePath.toFile().createNewFile());
            final Path path4 = getPath(tempFilename1, getSshPrivateKeyFileSearchPaths());
            Assertions.assertEquals(Paths.get(getDocumentdbHomePathName(), tempFilename1), path4);
        } finally {
            Assertions.assertTrue(documentDbTempFilePath != null && documentDbTempFilePath.toFile().delete());
        }

        // Test that it will use the .documentdb folder under the user's home path
        Path classPathParentTempFilePath = null;
        try {
            classPathParentTempFilePath = Paths.get(getClassPathLocationName(), tempFilename1);
            Assertions.assertTrue(classPathParentTempFilePath.toFile().createNewFile());
            final Path path5 = getPath(tempFilename1, getSshPrivateKeyFileSearchPaths());
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
}
