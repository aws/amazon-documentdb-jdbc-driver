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

import com.google.common.base.Strings;
import com.mongodb.MongoClientSettings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.DOCUMENTDB_CUSTOM_OPTIONS;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.ValidationType.SSH_TUNNEL;

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
        properties.setDefaultAuthenticationDatabase("test");
        properties.setAllowDiskUseOption("disable");

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
        Assertions.assertEquals("test", properties.getDefaultAuthenticationDatabase());
        Assertions.assertEquals(DocumentDbAllowDiskUseOption.DISABLE, properties.getAllowDiskUseOption());

        // Build sanitized connection string.
        Assertions.assertEquals(
                "//USER@HOSTNAME/DATABASE?appName=APPNAME"
                        + "&loginTimeoutSec=100"
                        + "&scanLimit=100"
                        + "&replicaSet=rs0"
                        + "&tlsAllowInvalidHostnames=true"
                        + "&tlsCAFile=src%2Fmain%2Fresources%2Frds-ca-2019-root.pem"
                        + "&sshUser=SSHUSER"
                        + "&sshHost=SSHHOST"
                        + "&sshPrivateKeyFile=%7E%2F.ssh%2Ftest-file-name.pem"
                        + "&sshStrictHostKeyChecking=false"
                        + "&sshKnownHostsFile=%7E%2F.ssh%2Funknown_hosts"
                        + "&defaultFetchSize=1000"
                        + "&refreshSchema=true"
                        + "&defaultAuthDb=test"
                        + "&allowDiskUse=disable",
                properties.buildSanitizedConnectionString());

        // Build client settings.
        final MongoClientSettings settings = properties.buildMongoClientSettings();
        Assertions.assertNotNull(settings);
        Assertions.assertEquals("USER", settings.getCredential().getUserName());
        Assertions.assertEquals("PASSWORD", String.valueOf(settings.getCredential().getPassword()));
        Assertions.assertEquals("test", settings.getCredential().getSource());
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
     * Tests setting the allow disk use option with the DocumentDbAllowDiskUseOption enum.
     */
    @Test
    @DisplayName("Tests setting the allow disk use option with the DocumentDbAllowDiskUseOption enum.")
    public void testAllowDiskUseOptions() {
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
        properties.setAllowDiskUseOption(DocumentDbAllowDiskUseOption.DEFAULT.getName());
        Assertions.assertEquals(DocumentDbAllowDiskUseOption.DEFAULT, properties.getAllowDiskUseOption());
        properties.setAllowDiskUseOption(DocumentDbAllowDiskUseOption.DISABLE.getName());
        Assertions.assertEquals(DocumentDbAllowDiskUseOption.DISABLE, properties.getAllowDiskUseOption());
        properties.setAllowDiskUseOption(DocumentDbAllowDiskUseOption.ENABLE.getName());
        Assertions.assertEquals(DocumentDbAllowDiskUseOption.ENABLE, properties.getAllowDiskUseOption());
        properties.setAllowDiskUseOption("garbage");
        Assertions.assertNull(properties.getAllowDiskUseOption());
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
                "&" + DocumentDbConnectionProperty.SCHEMA_NAME.getName() + "=" + "notDefault" +
                "&" + DocumentDbConnectionProperty.SSH_USER.getName() + "=" + "sshUser" +
                "&" + DocumentDbConnectionProperty.SSH_HOSTNAME.getName() + "=" + "sshHost" +
                "&" + DocumentDbConnectionProperty.SSH_PRIVATE_KEY_FILE.getName() + "=" + "~/.ssh/key.pem" +
                "&" + DocumentDbConnectionProperty.SSH_PRIVATE_KEY_PASSPHRASE.getName() + "=" + "passphrase" +
                "&" + DocumentDbConnectionProperty.SSH_STRICT_HOST_KEY_CHECKING.getName() + "=" + "false" +
                "&" + DocumentDbConnectionProperty.SSH_KNOWN_HOSTS_FILE.getName() + "=" + "~/.ssh/known_hosts" +
                "&" + DocumentDbConnectionProperty.DEFAULT_FETCH_SIZE.getName() + "=" + "1000" +
                "&" + DocumentDbConnectionProperty.REFRESH_SCHEMA.getName() + "=" + "true" +
                "&" + DocumentDbConnectionProperty.DEFAULT_AUTH_DB.getName() + "=" + "test" +
                "&" + DocumentDbConnectionProperty.ALLOW_DISK_USE.getName() + "=" + "disable";
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

    @DisplayName("Test that custom options are added.")
    @Test
    void testCustomOptions() throws Exception {
        final Properties info = new Properties();
        final Map<String, String> environment = new HashMap<>();
        environment.putAll(System.getenv());
        environment.put(DOCUMENTDB_CUSTOM_OPTIONS, "allowDiskUse=enable;unknownOption=true");
        try {
            setEnv(environment);

            final String connectionString = "jdbc:documentdb://username:password@127.0.0.1/database";
            final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                    .getPropertiesFromConnectionString(info, connectionString, DOCUMENT_DB_SCHEME);
            Assertions.assertEquals(6, properties.size());
            Assertions.assertEquals("127.0.0.1", properties.getProperty("host"));
            Assertions.assertEquals("database", properties.getProperty("database"));
            Assertions.assertEquals("username", properties.getProperty("user"));
            Assertions.assertEquals("password", properties.getProperty("password"));
            Assertions.assertEquals("enable", properties.getProperty("allowDiskUse"));
            Assertions.assertEquals("true", properties.getProperty("unknownOption"));
        } finally {
            // Restore the environment, so it doesn't affect other tests.
            environment.clear();
            environment.putAll(System.getenv());
            environment.remove(DOCUMENTDB_CUSTOM_OPTIONS);
            Assertions.assertEquals(null, environment.get(DOCUMENTDB_CUSTOM_OPTIONS));
            setEnv(environment);
            Assertions.assertEquals(null, System.getenv(DOCUMENTDB_CUSTOM_OPTIONS));
        }
    }

    /**
     * Resets the System's environment maps.
     *
     * https://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java
     *
     * @param newEnv the new map to replace the existing environment map.
     * @throws Exception if environment private fields are not found or are not as
     * previously implemented.
     */
    @SuppressWarnings("unchecked")
    protected static void setEnv(final Map<String, String> newEnv) throws Exception {
        try {
            final Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            final Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
            theEnvironmentField.setAccessible(true);
            final Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            env.clear();
            env.putAll(newEnv);
            final Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            theCaseInsensitiveEnvironmentField.setAccessible(true);
            final Map<String, String> ciEnv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
            ciEnv.clear();
            ciEnv.putAll(newEnv);
        } catch (NoSuchFieldException e) {
            final Class[] classes = Collections.class.getDeclaredClasses();
            final Map<String, String> env = System.getenv();
            for (final Class cl : classes) {
                if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                    final Field field = cl.getDeclaredField("m");
                    field.setAccessible(true);
                    final Object obj = field.get(env);
                    final Map<String, String> map = (Map<String, String>) obj;
                    map.clear();
                    map.putAll(newEnv);
                }
            }
        }
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

    /**
     * Tests getting and setting the application name.
     */
    @Test
    @DisplayName("Tests retrieving default and overridden application name and that the name is used in client settings.")
    public void testApplicationName() throws SQLException {
        // Get default app name.
        final Properties info = new Properties();
        final String connectionString = "jdbc:documentdb://username:password@localhost/database";
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, DOCUMENT_DB_SCHEME);
        Assertions.assertFalse(Strings.isNullOrEmpty(properties.getApplicationName()));
        Assertions.assertEquals(DocumentDbConnectionProperties.DEFAULT_APPLICATION_NAME, properties.getApplicationName());
        // Override app name.
        properties.setApplicationName("APPNAME");
        Assertions.assertEquals("APPNAME", properties.getApplicationName());
        // Build client settings and ensure app name is passed.
        final MongoClientSettings settings = properties.buildMongoClientSettings();
        Assertions.assertEquals("APPNAME", settings.getApplicationName());
    }

    /**
     * Tests getting and setting the default authentication database.
     */
    @Test
    @DisplayName("Tests retrieving default and overridden authentication database and that the database is used in client settings.")
    public void testDefaultAuthenticationDatabase() throws SQLException {
        // Get default authentication database.
        final Properties info = new Properties();
        final String connectionString = "jdbc:documentdb://username:password@localhost/database";
        final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(info, connectionString, DOCUMENT_DB_SCHEME);
        Assertions.assertEquals(DocumentDbConnectionProperty.DEFAULT_AUTH_DB.getDefaultValue(), properties.getDefaultAuthenticationDatabase());
        // Override test database.
        properties.setDefaultAuthenticationDatabase("test");
        Assertions.assertEquals("test", properties.getDefaultAuthenticationDatabase());
        // Build client settings and ensure authentication database is passed.
        final MongoClientSettings settings = properties.buildMongoClientSettings();
        Assertions.assertEquals("test", settings.getCredential().getSource());
    }

    @SuppressFBWarnings("HARD_CODE_PASSWORD")
    @Test
    @DisplayName("Tests that it can build just the SSH tunnel connection string.")
    void testBuildSshConnectionProperties() throws SQLException {
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
        properties.setDefaultAuthenticationDatabase("test");
        properties.setAllowDiskUseOption("disable");

        Assertions.assertEquals("//HOSTNAME/"
                        + "?sshUser=SSHUSER"
                        + "&sshHost=SSHHOST"
                        + "&sshPrivateKeyFile=%7E%2F.ssh%2Ftest-file-name.pem"
                        + "&sshPrivateKeyPassphrase=PASSPHRASE"
                        + "&sshStrictHostKeyChecking=false"
                        + "&sshKnownHostsFile=%7E%2F.ssh%2Funknown_hosts",
                properties.buildSshConnectionString());

        final DocumentDbConnectionProperties parsedProperties =
                DocumentDbConnectionProperties.getPropertiesFromConnectionString(
                        DOCUMENT_DB_SCHEME + properties.buildSshConnectionString(), SSH_TUNNEL);
        Assertions.assertEquals(properties.buildSshConnectionString(), parsedProperties.buildSshConnectionString());
    }

    static @NonNull String buildInternalSshTunnelConnectionString(
            final @NonNull DocumentDbTestEnvironment environment) throws SQLException {
        final DocumentDbConnectionProperties properties = DocumentDbConnectionTest
                .getInternalSSHTunnelProperties(environment);
        final String loginInfo = DocumentDbConnectionProperties.buildLoginInfo(
                properties.getUser(), properties.getPassword());
        final String hostInfo = DocumentDbConnectionProperties.buildHostInfo(properties.getHostname());
        final String databaseInfo = DocumentDbConnectionProperties.buildDatabaseInfo(properties.getDatabase());
        final StringBuilder optionalInfo = new StringBuilder();
        DocumentDbConnectionProperties.buildSanitizedOptionalInfo(optionalInfo, properties);
        DocumentDbConnectionProperties.maybeAppendOptionalValue(
                optionalInfo,
                DocumentDbConnectionProperty.SSH_PRIVATE_KEY_PASSPHRASE,
                properties.getSshPrivateKeyPassphrase(), null);
        return DOCUMENT_DB_SCHEME + DocumentDbConnectionProperties.buildConnectionString(
                loginInfo,
                hostInfo,
                databaseInfo,
                optionalInfo.toString());
    }
}
