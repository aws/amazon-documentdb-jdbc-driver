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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironmentFactory;
import software.amazon.documentdb.jdbc.persist.SchemaStoreFactory;
import software.amazon.documentdb.jdbc.persist.SchemaWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperty.SCHEMA_NAME;
import static software.amazon.documentdb.jdbc.DocumentDbMain.COMPLETE_OPTIONS;
import static software.amazon.documentdb.jdbc.DocumentDbMain.tryGetConnectionProperties;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.DEFAULT_SCHEMA_NAME;

class DocumentDbMainTest {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final Pattern MONGO_OUTPUT_PATTERN = Pattern
            .compile("^.*\\[mongod output].*$", Pattern.MULTILINE | Pattern.DOTALL);
    private ByteArrayOutputStream outputStream;
    private DocumentDbConnectionProperties properties;

    private static Stream<DocumentDbTestEnvironment> getTestEnvironments() {
        return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream();
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        for (DocumentDbTestEnvironment environment : getTestEnvironments()
                .collect(Collectors.toList())) {
            environment.start();
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        for (DocumentDbTestEnvironment environment : getTestEnvironments()
                .collect(Collectors.toList())) {
            final DocumentDbConnectionProperties properties = DocumentDbConnectionProperties
                    .getPropertiesFromConnectionString(environment.getJdbcConnectionString());
            final SchemaWriter writer = SchemaStoreFactory.createWriter(properties);
            writer.remove(DEFAULT_SCHEMA_NAME);
            environment.stop();
        }
    }

    @BeforeEach
    void beforeEach() throws IOException {
        outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(
                outputStream, true, DEFAULT_CHARSET.name()));
    }

    @DisplayName("Tests empty command line with no options provided.")
    @Test
    void testEmptyCommandLine() throws IOException {
        DocumentDbMain.main(new String[] {});
        Assertions.assertTrue(getConsoleOutput().endsWith(
                "Missing required options: "
                        + "[-g Generates a new schema for the database. This will have the effect of replacing an existing schema of the same name, if it exists., "
                        + "-r Removes the schema from storage for schema given by -m <schema-name>, or for schema '_default', if not provided.], "
                        + "s, d, u"));
    }

    @ParameterizedTest(name = "testMinimum - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testMinimum(final DocumentDbTestEnvironment testEnvironment)
            throws ParseException, SQLException {
        setConnectionProperties(testEnvironment);
        final String password = UUID.randomUUID().toString();
        final String[] args = new String[]{
                "-g",
                "-s", "localhost",
                "-d", "database",
                "-u", "user",
                "-p", password
        };

        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
        final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties));
        Assertions.assertEquals("localhost", newProperties.getHostname());
        Assertions.assertEquals("database", newProperties.getDatabase());
        Assertions.assertEquals("user", newProperties.getUser());
        Assertions.assertEquals(SCHEMA_NAME.getDefaultValue(), newProperties.getSchemaName());
        Assertions.assertFalse(newProperties.getTlsEnabled());
        Assertions.assertFalse(newProperties.getTlsAllowInvalidHostnames());
    }

    @ParameterizedTest(name = "testMinimumLongArgs - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testMinimumLongArgs(final DocumentDbTestEnvironment testEnvironment)
            throws ParseException, SQLException {
        setConnectionProperties(testEnvironment);
        final String password = UUID.randomUUID().toString();
        final String[] args = new String[] {
                "--generate-new",
                "--server",  "localhost",
                "--database", "database",
                "--user", "user",
                "--password", password
        };
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
        final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties));
        Assertions.assertEquals("localhost", newProperties.getHostname());
        Assertions.assertEquals("database", newProperties.getDatabase());
        Assertions.assertEquals("user", newProperties.getUser());
        Assertions.assertEquals(SCHEMA_NAME.getDefaultValue(), newProperties.getSchemaName());
        Assertions.assertFalse(newProperties.getTlsEnabled());
        Assertions.assertFalse(newProperties.getTlsAllowInvalidHostnames());
    }

    @ParameterizedTest(name = "testMinimumAssignedLong - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testMinimumAssignedLong(final DocumentDbTestEnvironment testEnvironment)
            throws ParseException, SQLException {
        setConnectionProperties(testEnvironment);
        final String password = UUID.randomUUID().toString();
        final String[] args = new String[] {
                "--generate-new",
                "--server=localhost",
                "--database=database",
                "--user=user",
                "--password=" + password
        };
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
        final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties));
        Assertions.assertEquals("localhost", newProperties.getHostname());
        Assertions.assertEquals("database", newProperties.getDatabase());
        Assertions.assertEquals("user", newProperties.getUser());
        Assertions.assertEquals(SCHEMA_NAME.getDefaultValue(), newProperties.getSchemaName());
        Assertions.assertFalse(newProperties.getTlsEnabled());
        Assertions.assertFalse(newProperties.getTlsAllowInvalidHostnames());
    }

    @ParameterizedTest(name = "testGenerateNew - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testGenerateNew(final DocumentDbTestEnvironment testEnvironment)
            throws ParseException, SQLException, UnsupportedEncodingException {
        setConnectionProperties(testEnvironment);
        final String[] args =
                String.format(
                        "-g -s=%s -d=%s -u=%s -p=%s%s%s",
                        properties.getHostname(),
                        properties.getDatabase(),
                        properties.getUser(),
                        properties.getPassword(),
                        properties.getTlsEnabled() ? " -t" : "",
                        properties.getTlsAllowInvalidHostnames() ? " -a" : "").split(" ");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
        final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties));
        Assertions.assertEquals(properties.getHostname(), newProperties.getHostname());
        Assertions.assertEquals(properties.getDatabase(), newProperties.getDatabase());
        Assertions.assertEquals(properties.getUser(), newProperties.getUser());
        Assertions.assertEquals(SCHEMA_NAME.getDefaultValue(), newProperties.getSchemaName());
        Assertions.assertEquals(properties.getTlsEnabled(), newProperties.getTlsEnabled());
        Assertions.assertEquals(properties.getTlsAllowInvalidHostnames(), newProperties.getTlsAllowInvalidHostnames());

        DocumentDbMain.main(args);
        final String output = getConsoleOutput();
        Assertions.assertTrue(output.endsWith("New schema '_default', version '1' generated."));
    }

    @ParameterizedTest(name = "testRemove - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testRemove(final DocumentDbTestEnvironment testEnvironment)
            throws ParseException, SQLException, IOException {
        setConnectionProperties(testEnvironment);
        final String[] args =
                String.format(
                        "-r -s=%s -d=%s -u=%s -p=%s%s%s",
                        properties.getHostname(),
                        properties.getDatabase(),
                        properties.getUser(),
                        properties.getPassword(),
                        properties.getTlsEnabled() ? " -t" : "",
                        properties.getTlsAllowInvalidHostnames() ? " -a" : "").split(" ");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
        final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties));
        Assertions.assertEquals(properties.getHostname(), newProperties.getHostname());
        Assertions.assertEquals(properties.getDatabase(), newProperties.getDatabase());
        Assertions.assertEquals(properties.getUser(), newProperties.getUser());
        Assertions.assertEquals(SCHEMA_NAME.getDefaultValue(), newProperties.getSchemaName());
        Assertions.assertEquals(properties.getTlsEnabled(), newProperties.getTlsEnabled());
        Assertions.assertEquals(properties.getTlsAllowInvalidHostnames(), newProperties.getTlsAllowInvalidHostnames());

        DocumentDbMain.main(args);
        final String output = getConsoleOutput();
        Assertions.assertTrue(output.endsWith("Removed schema '_default'."));
    }

    @DisplayName("Tests it detects an \"unknown\" option")
    @Test
    void testUnrecognizedOption()
            throws UnsupportedEncodingException {
        DocumentDbMain.main(new String[] {"-x", "-g", "-s=localhost", "-d=test", "-u=testuser", "-p=password"});
        Assertions.assertTrue(getConsoleOutput().endsWith("Unrecognized option: -x"));
    }

    @DisplayName("Tests the help (--help) option")
    @Test
    void testHelpOption()
            throws UnsupportedEncodingException {
        DocumentDbMain.main(new String[] {"--help"});
        Assertions.assertTrue(getConsoleOutput().endsWith(
                "usage: main\n"
                        + " -a,--tls-allow-invalid-hostnames   The indicator of whether to allow\n"
                        + "                                    invalid hostnames when connecting to\n"
                        + "                                    DocumentDB. Default: false.\n"
                        + " -d,--database <database-name>      The name of the database for the\n"
                        + "                                    schema operations. Required.\n"
                        + " -g,--generate-new                  Generates a new schema for the\n"
                        + "                                    database. This will have the effect of\n"
                        + "                                    replacing an existing schema of the\n"
                        + "                                    same name, if it exists.\n"
                        + " -h,--help                          Prints the command line syntax.\n"
                        + " -l,--scan-limit <max-documents>    The maximum number of documents to\n"
                        + "                                    sample in each collection. Used in\n"
                        + "                                    conjunction with the --generate-new\n"
                        + "                                    command. Default: 1000.\n"
                        + " -m,--scan-method <method>          The scan method to sample documents\n"
                        + "                                    from the collections. One of: random,\n"
                        + "                                    idForward, idReverse, or all. Used in\n"
                        + "                                    conjunction with the --generate-new\n"
                        + "                                    command. Default: random.\n"
                        + " -n,--schema-name <schema-name>     The name of the schema. Default:\n"
                        + "                                    _default.\n"
                        + " -p,--password <password>           The password for the user performing\n"
                        + "                                    the schema operations. Optional. If\n"
                        + "                                    this option is not provided, the\n"
                        + "                                    end-user will be prompted to enter the\n"
                        + "                                    password directly.\n"
                        + " -r,--remove                        Removes the schema from storage for\n"
                        + "                                    schema given by -m <schema-name>, or\n"
                        + "                                    for schema '_default', if not\n"
                        + "                                    provided.\n"
                        + " -s,--server <host-name>            The hostname and optional port number\n"
                        + "                                    (default: 27017) in the format\n"
                        + "                                    hostname[:port]. Required.\n"
                        + " -t,--tls                           The indicator of whether to use TLS\n"
                        + "                                    encryption when connecting to\n"
                        + "                                    DocumentDB. Default: false.\n"
                        + " -u,--user <user-name>              The name of the user performing the\n"
                        + "                                    schema operations. Required. Note: the\n"
                        + "                                    user will require readWrite role on\n"
                        + "                                    the <database-name> where the schema\n"
                        + "                                    are stored if creating or modifying\n"
                        + "                                    schema.\n"
                        + "    --version                       Prints the version number of the\n"
                        + "                                    command."));
    }

    @DisplayName("Tests the version (--version) option")
    @Test
    void testVersionOption()
            throws UnsupportedEncodingException {
        DocumentDbMain.main(new String[] {"--version"});
        Assertions.assertTrue(getConsoleOutput().endsWith(String.format(
                "%s: version %s", DocumentDbMain.LIBRARY_NAME, DocumentDbMain.ARCHIVE_VERSION)));
    }

    private String getConsoleOutput() throws UnsupportedEncodingException {
        return outputStream.toString(DEFAULT_CHARSET.name()).replace("\r\n", "\n").trim();
    }

    private void setConnectionProperties(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        this.properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(testEnvironment.getJdbcConnectionString());
    }
}
