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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironment;
import software.amazon.documentdb.jdbc.common.test.DocumentDbTestEnvironmentFactory;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaWriter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Strings.isNullOrEmpty;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.USER_HOME_PROPERTY;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperty.SCHEMA_NAME;
import static software.amazon.documentdb.jdbc.DocumentDbMain.COMPLETE_OPTIONS;
import static software.amazon.documentdb.jdbc.DocumentDbMain.tryGetConnectionProperties;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.DEFAULT_SCHEMA_NAME;
import static software.amazon.documentdb.jdbc.persist.DocumentDbSchemaReader.TABLE_SCHEMA_COLLECTION;

class DocumentDbMainTest {

    // Ensure custom schema can be sorted after "_default" - so start with a lower-case letter
    private static final String CUSTOM_SCHEMA_NAME = "a" + UUID.randomUUID();
    public static final String NEW_DEFAULT_SCHEMA_ANY_VERSION_REGEX =
            Pattern.quote("New schema '_default', version '")
                    + "\\d+"
                    + Pattern.quote("' generated.");
    public static final Pattern NEW_DEFAULT_SCHEMA_ANY_VERSION_PATTERN = Pattern
            .compile(NEW_DEFAULT_SCHEMA_ANY_VERSION_REGEX);
    private DocumentDbConnectionProperties properties;
    public static final Path USER_HOME_PATH = Paths.get(System.getProperty(USER_HOME_PROPERTY));
    private static final String DOC_DB_PRIV_KEY_FILE_PROPERTY = "DOC_DB_PRIV_KEY_FILE";
    private static final String DOC_DB_USER_PROPERTY = "DOC_DB_USER";
    private static final String DOC_DB_HOST_PROPERTY = "DOC_DB_HOST";

    private static Stream<DocumentDbTestEnvironment> getTestEnvironments() {
        return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream();
    }

    private static Stream<DocumentDbTestEnvironment> getDocumentDb40SshTunnelEnvironmentSourceOrNull() {
        if (DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream()
                .anyMatch(e -> e ==  DocumentDbTestEnvironmentFactory
                        .getDocumentDb40SshTunnelEnvironment())) {
            return DocumentDbTestEnvironmentFactory.getConfiguredEnvironments().stream()
                    .filter(e -> e == DocumentDbTestEnvironmentFactory
                            .getDocumentDb40SshTunnelEnvironment());
        } else {
            return Stream.of((DocumentDbTestEnvironment) null);
        }
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        for (DocumentDbTestEnvironment environment : getTestEnvironments()
                .collect(Collectors.toList())) {
            environment.start();
        }
    }

    @AfterEach
    void afterEach() {
        if (properties != null) {
            try (DocumentDbSchemaWriter writer = new DocumentDbSchemaWriter(properties, null)) {
                writer.remove(DEFAULT_SCHEMA_NAME);
                writer.remove(CUSTOM_SCHEMA_NAME);
            }
        }
        properties = null;
    }

    @AfterAll
    static void afterAll() throws Exception {
        for (DocumentDbTestEnvironment environment : getTestEnvironments()
                .collect(Collectors.toList())) {
            environment.stop();
        }
    }

    @DisplayName("Tests empty command line with no options provided.")
    @Test
    void testEmptyCommandLine() throws SQLException {
        final StringBuilder output = new StringBuilder();
        DocumentDbMain.handleCommandLine(new String[] {}, output);
        Assertions.assertEquals(
                "Missing required options: [-g Generates a new schema for the database. This will have the effect of replacing an existing schema of the same name, if it exists., -r Removes the schema from storage for schema given by -m <schema-name>, or for schema '_default', if not provided., -l Lists the schema names, version and table names available in the schema repository., -b Lists the SQL table names in a schema., -e Exports the schema to for SQL tables named [<table-name>[,<table-name>[…]]]. If no <table-name> are given, all table schema will be exported. By default, the schema is written to stdout. Use the --output option to write to a file. The output format is JSON., -i Imports the schema from <file-name> in your home directory. The schema will be imported using the <schema-name> and a new version will be added - replacing the existing schema. The expected input format is JSON.], s, d, u\n"
                        + "usage: " + DocumentDbMain.getLibraryName() + " [-g | -r | -l | -b | -e <[table-name[,...]]> |\n"
                        + "                                 -i <file-name>] -s <host-name> -d\n"
                        + "                                 <database-name> -u <user-name> [-p <password>]\n"
                        + "                                 [-n <schema-name>] [-m <method>] [-x\n"
                        + "                                 <max-documents>] [-t] [-a] [-o <file-name>]\n"
                        + "                                 [-h] [--version]\n"
                        + " -a,--tls-allow-invalid-hostnames  The indicator of whether to allow invalid\n"
                        + "                                   hostnames when connecting to DocumentDB.\n"
                        + "                                   Default: false.\n"
                        + " -b,--list-tables                  Lists the SQL table names in a schema.\n"
                        + " -d,--database <database-name>     The name of the database for the schema\n"
                        + "                                   operations. Required.\n"
                        + " -e,--export <[table-name[,...]]>  Exports the schema to for SQL tables named\n"
                        + "                                   [<table-name>[,<table-name>[…]]]. If no\n"
                        + "                                   <table-name> are given, all table schema will\n"
                        + "                                   be exported. By default, the schema is\n"
                        + "                                   written to stdout. Use the --output option to\n"
                        + "                                   write to a file. The output format is JSON.\n"
                        + " -g,--generate-new                 Generates a new schema for the database. This\n"
                        + "                                   will have the effect of replacing an existing\n"
                        + "                                   schema of the same name, if it exists.\n"
                        + " -h,--help                         Prints the command line syntax.\n"
                        + " -i,--import <file-name>           Imports the schema from <file-name> in your\n"
                        + "                                   home directory. The schema will be imported\n"
                        + "                                   using the <schema-name> and a new version\n"
                        + "                                   will be added - replacing the existing\n"
                        + "                                   schema. The expected input format is JSON.\n"
                        + " -l,--list-schema                  Lists the schema names, version and table\n"
                        + "                                   names available in the schema repository.\n"
                        + " -m,--scan-method <method>         The scan method to sample documents from the\n"
                        + "                                   collections. One of: random, idForward,\n"
                        + "                                   idReverse, or all. Used in conjunction with\n"
                        + "                                   the --generate-new command. Default: random.\n"
                        + " -n,--schema-name <schema-name>    The name of the schema. Default: _default.\n"
                        + " -o,--output <file-name>           Write the exported schema to <file-name> in\n"
                        + "                                   your home directory (instead of stdout). This\n"
                        + "                                   will overwrite any existing file with the\n"
                        + "                                   same name\n"
                        + " -p,--password <password>          The password for the user performing the\n"
                        + "                                   schema operations. Optional. If this option\n"
                        + "                                   is not provided, the end-user will be\n"
                        + "                                   prompted to enter the password directly.\n"
                        + " -r,--remove                       Removes the schema from storage for schema\n"
                        + "                                   given by -m <schema-name>, or for schema\n"
                        + "                                   '_default', if not provided.\n"
                        + " -s,--server <host-name>           The hostname and optional port number\n"
                        + "                                   (default: 27017) in the format\n"
                        + "                                   hostname[:port]. Required.\n"
                        + " -t,--tls                          The indicator of whether to use TLS\n"
                        + "                                   encryption when connecting to DocumentDB.\n"
                        + "                                   Default: false.\n"
                        + " -u,--user <user-name>             The name of the user performing the schema\n"
                        + "                                   operations. Required. Note: the user will\n"
                        + "                                   require readWrite role on the <database-name>\n"
                        + "                                   where the schema are stored if creating or\n"
                        + "                                   modifying schema.\n"
                        + "    --version                      Prints the version number of the command.\n"
                        + " -x,--scan-limit <max-documents>   The maximum number of documents to sample in\n"
                        + "                                   each collection. Used in conjunction with the\n"
                        + "                                   --generate-new command. Default: 1000.\n",
                output.toString().replaceAll("\r\n", "\n"));
    }

    @Test()
    @DisplayName("Tests short option names for minimum set of options.")
    void testMinimum()
            throws ParseException {
        final String password = UUID.randomUUID().toString();
        final String[] args = new String[]{
                "-g",
                "-s", "localhost",
                "-d", "database",
                "-u", "user",
                "-p", password
        };

        final StringBuilder output = new StringBuilder();
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
        final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties, output));
        Assertions.assertEquals("localhost", newProperties.getHostname());
        Assertions.assertEquals("database", newProperties.getDatabase());
        Assertions.assertEquals("user", newProperties.getUser());
        Assertions.assertEquals(SCHEMA_NAME.getDefaultValue(), newProperties.getSchemaName());
        Assertions.assertFalse(newProperties.getTlsEnabled());
        Assertions.assertFalse(newProperties.getTlsAllowInvalidHostnames());
    }

    @Test()
    @DisplayName("Tests minimum long version option names")
    void testMinimumLongArgs()
            throws ParseException {
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
        final StringBuilder output = new StringBuilder();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties, output));
        Assertions.assertEquals("localhost", newProperties.getHostname());
        Assertions.assertEquals("database", newProperties.getDatabase());
        Assertions.assertEquals("user", newProperties.getUser());
        Assertions.assertEquals(SCHEMA_NAME.getDefaultValue(), newProperties.getSchemaName());
        Assertions.assertFalse(newProperties.getTlsEnabled());
        Assertions.assertFalse(newProperties.getTlsAllowInvalidHostnames());
    }

    @Test()
    @DisplayName("Tests long option name using assignment for arguments")
    void testMinimumAssignedLong()
            throws ParseException {
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
        final StringBuilder output = new StringBuilder();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties, output));
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
            throws ParseException, SQLException {
        setConnectionProperties(testEnvironment);
        final String[] args = buildArguments("-g", CUSTOM_SCHEMA_NAME);
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
        final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties();
        final StringBuilder output = new StringBuilder();
        Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties, output));
        Assertions.assertEquals(properties.getHostname(), newProperties.getHostname());
        Assertions.assertEquals(properties.getDatabase(), newProperties.getDatabase());
        Assertions.assertEquals(properties.getUser(), newProperties.getUser());
        Assertions.assertEquals(CUSTOM_SCHEMA_NAME, newProperties.getSchemaName());
        Assertions.assertEquals(properties.getTlsEnabled(), newProperties.getTlsEnabled());
        Assertions.assertEquals(properties.getTlsAllowInvalidHostnames(),
                newProperties.getTlsAllowInvalidHostnames());

        DocumentDbMain.handleCommandLine(args, output);
        Assertions.assertEquals(String.format("New schema '%s', version '1' generated.", CUSTOM_SCHEMA_NAME),
                output.toString());
    }

    @ParameterizedTest(name = "testRemove - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testRemove(final DocumentDbTestEnvironment testEnvironment)
            throws ParseException, SQLException {
        setConnectionProperties(testEnvironment);
        final String collectionName1 = createSimpleCollection(testEnvironment);
        final String collectionName2 = createSimpleCollection(testEnvironment);

        try {
            final StringBuilder output = new StringBuilder();
            String[] args = buildArguments("-g", CUSTOM_SCHEMA_NAME);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(
                    String.format("New schema '%s', version '1' generated.", CUSTOM_SCHEMA_NAME),
                    output.toString());

            args = buildArguments("-r", CUSTOM_SCHEMA_NAME);
            final CommandLineParser parser = new DefaultParser();
            final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
            final DocumentDbConnectionProperties newProperties = new DocumentDbConnectionProperties();

            Assertions.assertTrue(tryGetConnectionProperties(commandLine, newProperties, output));
            Assertions.assertEquals(properties.getHostname(), newProperties.getHostname());
            Assertions.assertEquals(properties.getDatabase(), newProperties.getDatabase());
            Assertions.assertEquals(properties.getUser(), newProperties.getUser());
            Assertions.assertEquals(CUSTOM_SCHEMA_NAME, newProperties.getSchemaName());
            Assertions.assertEquals(properties.getTlsEnabled(), newProperties.getTlsEnabled());
            Assertions.assertEquals(properties.getTlsAllowInvalidHostnames(),
                    newProperties.getTlsAllowInvalidHostnames());

            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(String.format("Removed schema '%s'.", CUSTOM_SCHEMA_NAME),
                    output.toString());

            output.setLength(0);
            args = buildArguments("-g", CUSTOM_SCHEMA_NAME);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(
                    String.format("New schema '%s', version '1' generated.", CUSTOM_SCHEMA_NAME),
                    output.toString());

            // drop the table schemas to be inconsistent.
            dropCollection(testEnvironment, TABLE_SCHEMA_COLLECTION);
            args = buildArguments("-r", CUSTOM_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(String.format("Removed schema '%s'.", CUSTOM_SCHEMA_NAME),
                    output.toString());
        } finally {
            dropCollection(testEnvironment, collectionName1);
            dropCollection(testEnvironment, collectionName2);
        }
    }

    @ParameterizedTest(name = "testListSchema - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testListSchema(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        setConnectionProperties(testEnvironment);
        final String collectionName1 = createSimpleCollection(testEnvironment);
        final String collectionName2 = createSimpleCollection(testEnvironment);

        try {
            String[] args = buildArguments("-g");
            final StringBuilder output = new StringBuilder();
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertTrue(NEW_DEFAULT_SCHEMA_ANY_VERSION_PATTERN
                    .matcher(output.toString())
                    .matches());

            args = buildArguments("-g", CUSTOM_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions
                    .assertEquals(String.format("New schema '%s', version '1' generated.", CUSTOM_SCHEMA_NAME),
                            output.toString());

            args = buildArguments("-l");
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(String.format(
                    "Name=%1$s, Version=1, SQL Name=%3$s%n"
                            + "Name=%2$s, Version=1, SQL Name=%3$s%n",
                    DEFAULT_SCHEMA_NAME,
                    CUSTOM_SCHEMA_NAME,
                    testEnvironment.getDatabaseName()),
                    output.toString().replaceAll(", Modified=.*", ""));

            // Ensure listing schemas doesn't create a new schema
            args = buildArguments("-r", CUSTOM_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(String.format("Removed schema '%s'.", CUSTOM_SCHEMA_NAME), output.toString());
            args = buildArguments("-r", DEFAULT_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(String.format("Removed schema '%s'.", DEFAULT_SCHEMA_NAME), output.toString());
            args = buildArguments("-l", CUSTOM_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(0, output.length());
        } finally {
            dropCollection(testEnvironment, collectionName1);
            dropCollection(testEnvironment, collectionName2);
        }
    }

    @ParameterizedTest(name = "testListTables - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testListTables(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        setConnectionProperties(testEnvironment);
        final String collectionName1 = createSimpleCollection(testEnvironment);
        final String collectionName2 = createSimpleCollection(testEnvironment);

        try {
            String[] args = buildArguments("-g");
            final StringBuilder output = new StringBuilder();
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertTrue(NEW_DEFAULT_SCHEMA_ANY_VERSION_PATTERN
                    .matcher(output.toString())
                    .matches());

            args = buildArguments("-g", CUSTOM_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions
                    .assertEquals("New schema '" + CUSTOM_SCHEMA_NAME + "', version '1' generated.",
                            output.toString());

            args = buildArguments("-b");
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            final List<String> formatArgs = Arrays
                    .stream(new String[]{collectionName1, collectionName2}).sorted()
                    .collect(Collectors.toList());
            String actual = output.toString().replace("\r\n", "\n");
            Assertions.assertEquals(String.format(
                      "%s\n"
                    + "%s\n",
                    formatArgs.toArray()),
                    actual);

            args = buildArguments("-b", CUSTOM_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            actual = output.toString().replace("\r\n", "\n");
            formatArgs.addAll(Arrays.stream(new String[]{collectionName1, collectionName2}).sorted()
                    .collect(Collectors.toList()));
            Assertions.assertEquals(String.format(
                      "%s\n"
                    + "%s\n",
                    formatArgs.toArray()),
                    actual);

            // Ensure listing tables doesn't create a new schema
            args = buildArguments("-r", CUSTOM_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(String.format("Removed schema '%s'.", CUSTOM_SCHEMA_NAME), output.toString());
            args = buildArguments("-b", CUSTOM_SCHEMA_NAME);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(0, output.length());
        } finally {
            dropCollection(testEnvironment, collectionName1);
            dropCollection(testEnvironment, collectionName2);
        }
    }

    @ParameterizedTest(name = "testListEmpty - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testListEmpty(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        setConnectionProperties(testEnvironment);

        final StringBuilder output = new StringBuilder();
        final String[] args = buildArguments("-l");
        DocumentDbMain.handleCommandLine(args, output);
        Assertions.assertEquals(0, output.length());
    }

    @ParameterizedTest(name = "testExportStdOut - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testExportStdOut(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        setConnectionProperties(testEnvironment);
        final String collectionName1 = createSimpleCollection(testEnvironment);
        final String collectionName2 = createSimpleCollection(testEnvironment);

        try {

            String[] args = buildArguments("-g");
            final StringBuilder output = new StringBuilder();
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertTrue(NEW_DEFAULT_SCHEMA_ANY_VERSION_PATTERN
                    .matcher(output.toString())
                    .matches());

            final String[] collections = Arrays
                    .stream(new String[] { collectionName1, collectionName2})
                    .sorted()
                    .collect(Collectors.toList()).toArray(new String[2]);
            args = buildArguments(String.format("-e=%s,%s", (Object[]) collections));
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(
                    getExpectedExportContent(collections),
                    output.toString().replace("\r\n", "\n"));
        } finally {
            dropCollection(testEnvironment, collectionName2);
            dropCollection(testEnvironment, collectionName1);
        }
    }

    @ParameterizedTest(name = "testExportOutputFile - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testExportOutputFile(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException, IOException {
        setConnectionProperties(testEnvironment);
        final String collectionName = createSimpleCollection(testEnvironment);

        try {
            String[] args = buildArguments("-g", DEFAULT_SCHEMA_NAME);
            final StringBuilder output = new StringBuilder();
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertTrue(NEW_DEFAULT_SCHEMA_ANY_VERSION_PATTERN
                    .matcher(output.toString())
                    .matches());

            final String outputFileName = collectionName + " tableSchema.json";
            final Path outputFilePath = USER_HOME_PATH.resolve(outputFileName);
            output.setLength(0);
            args = buildArguments("-e=" + collectionName, DEFAULT_SCHEMA_NAME, outputFileName);
            try {
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals("",
                        output.toString().replace("\r\n", "\n"));
                readOutputFileContent(outputFilePath, output);
                Assertions.assertEquals(
                        getExpectedExportContent(collectionName),
                        output.toString().replace("\r\n", "\n"));
            } finally {
                Assertions.assertTrue(outputFilePath.toFile().delete());
            }
        } finally {
            dropCollection(testEnvironment, collectionName);
        }
    }

    @ParameterizedTest(name = "testImportFile - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testImportFile(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException, IOException {
        setConnectionProperties(testEnvironment);
        final String collectionName = createSimpleCollection(testEnvironment);

        try {
            String[] args = buildArguments("-g", CUSTOM_SCHEMA_NAME);
            final StringBuilder output = new StringBuilder();
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(String.format("New schema '%s', version '1' generated.", CUSTOM_SCHEMA_NAME),
                    output.toString());

            final String outputFileName = collectionName + "_tableSchema.json";
            final Path outputFilePath = USER_HOME_PATH.resolve(outputFileName);
            output.setLength(0);
            args = buildArguments("-e=" + collectionName, CUSTOM_SCHEMA_NAME, outputFileName);
            try {
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals("",
                        output.toString().replace("\r\n", "\n"));
                readOutputFileContent(outputFilePath, output);
                Assertions.assertEquals(
                        getExpectedExportContent(collectionName),
                        output.toString().replace("\r\n", "\n"));

                output.setLength(0);
                args = buildArguments("-r", CUSTOM_SCHEMA_NAME);
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals(String.format("Removed schema '%s'.", CUSTOM_SCHEMA_NAME), output.toString());

                output.setLength(0);
                args = buildArguments("-b", CUSTOM_SCHEMA_NAME);
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals(0, output.length());

                output.setLength(0);
                args = buildArguments("-i=" + outputFileName, CUSTOM_SCHEMA_NAME);
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals("", output.toString());

                output.setLength(0);
                args = buildArguments("-b", CUSTOM_SCHEMA_NAME);
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals(collectionName, output.toString().trim());
            } finally {
                Assertions.assertTrue(outputFilePath.toFile().delete());
            }
        } finally {
            dropCollection(testEnvironment, collectionName);
        }
    }

    @ParameterizedTest(name = "testImportFileDuplicateColumn - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testImportFileDuplicateColumn(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException, IOException {
        setConnectionProperties(testEnvironment);
        final String collectionName = createSimpleCollection(testEnvironment);

        try {
            String[] args = buildArguments("-g", DEFAULT_SCHEMA_NAME);
            final StringBuilder output = new StringBuilder();
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertTrue(NEW_DEFAULT_SCHEMA_ANY_VERSION_PATTERN
                    .matcher(output.toString())
                    .matches());

            final String outputFileName = collectionName + " tableSchema.json";
            final Path outputFilePath = USER_HOME_PATH.resolve(outputFileName);
            output.setLength(0);
            args = buildArguments("-e=" + collectionName, DEFAULT_SCHEMA_NAME, outputFileName);
            try {
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals("",
                        output.toString().replace("\r\n", "\n"));
                readOutputFileContent(outputFilePath, output);
                Assertions.assertEquals(
                        getExpectedExportContent(collectionName),
                        output.toString().replace("\r\n", "\n"));

                final String outputWithDuplicateColumnName = getExpectedExportContent(
                        collectionName)
                        .replace("\"sqlName\" : \"fieldDouble\"",
                                "\"sqlName\" : \"fieldString\"");
                try (BufferedWriter bufferedWriter = Files
                        .newBufferedWriter(outputFilePath, StandardCharsets.UTF_8)) {
                    bufferedWriter.write(outputWithDuplicateColumnName);
                }

                output.setLength(0);
                args = buildArguments("-i=" + outputFileName, DEFAULT_SCHEMA_NAME);
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals(
                        String.format("Duplicate column key 'fieldString' detected for"
                                        + " table schema '%s'."
                                        + " Original column 'DocumentDbSchemaColumn{fieldPath='fieldDouble',"
                                        + " sqlName='fieldString', sqlType=DOUBLE, dbType=DOUBLE, index=false,"
                                        + " primaryKey=false, foreignKeyTableName='null', foreignKeyColumnName='null'}'."
                                        + " Duplicate column 'DocumentDbSchemaColumn{fieldPath='fieldString',"
                                        + " sqlName='fieldString', sqlType=VARCHAR, dbType=STRING, index=false,"
                                        + " primaryKey=false, foreignKeyTableName='null', foreignKeyColumnName='null'}'.",
                                collectionName),
                        output.toString());
            } finally {
                Assertions.assertTrue(outputFilePath.toFile().delete());
            }
        } finally {
            dropCollection(testEnvironment, collectionName);
        }
    }

    @ParameterizedTest(name = "testImportUnauthorizedError - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testImportUnauthorizedError(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException, IOException {
        setConnectionProperties(testEnvironment);
        final String collectionName = createSimpleCollection(testEnvironment);

        try {
            String[] args = buildArguments("-g", DEFAULT_SCHEMA_NAME);
            final StringBuilder output = new StringBuilder();
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertTrue(NEW_DEFAULT_SCHEMA_ANY_VERSION_PATTERN
                    .matcher(output.toString())
                    .matches());

            final String outputFileName = collectionName + " tableSchema.json";
            final Path outputFilePath = USER_HOME_PATH.resolve(outputFileName);
            output.setLength(0);
            args = buildArguments("-e=" + collectionName, DEFAULT_SCHEMA_NAME, outputFileName);
            try {
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals("",
                        output.toString().replace("\r\n", "\n"));
                readOutputFileContent(outputFilePath, output);
                Assertions.assertEquals(
                        getExpectedExportContent(collectionName),
                        output.toString().replace("\r\n", "\n"));

                output.setLength(0);
                args = buildArguments("-i=" + outputFileName,
                        DEFAULT_SCHEMA_NAME,
                        null,
                        DocumentDbConnectionProperties
                                .getPropertiesFromConnectionString(
                                        testEnvironment.getRestrictedUserConnectionString()));
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertTrue(output.toString().contains("Command failed with error 13"));
            } finally {
                Assertions.assertTrue(outputFilePath.toFile().delete());
            }
        } finally {
            dropCollection(testEnvironment, collectionName);
        }
    }

    @ParameterizedTest(name = "testExportInvalidTable - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testExportInvalidTable(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        setConnectionProperties(testEnvironment);
        final String collectionName = createSimpleCollection(testEnvironment);

        try {
            String[] args = buildArguments("-g");
            final StringBuilder output = new StringBuilder();
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertTrue(NEW_DEFAULT_SCHEMA_ANY_VERSION_PATTERN
                    .matcher(output.toString())
                    .matches());

            final String invalidTableName = UUID.randomUUID().toString();
            args = buildArguments("-e=" + invalidTableName);
            output.setLength(0);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertTrue(output.toString().replace("\r\n", "\n").startsWith(
                    "Requested table name(s) are not recognized in schema: " + invalidTableName
                            + "\n"
                            + "Available table names: "));
        } finally {
            dropCollection(testEnvironment, collectionName);
        }
    }

    @DisplayName("Tests it detects an \"Unrecognized\" option")
    @Test
    void testUnrecognizedOption() throws SQLException {
        final StringBuilder output = new StringBuilder();
        DocumentDbMain.handleCommandLine(
                new String[] {"-w", "-g", "-s=localhost", "-d=test", "-u=testuser", "-p=password"},
                output);
        Assertions.assertEquals("Unrecognized option: -w", output.toString());
    }

    @DisplayName("Tests the help (--help) option")
    @Test
    void testHelpOption() throws SQLException {
        final StringBuilder output = new StringBuilder();
        DocumentDbMain.handleCommandLine(new String[] {"--help"}, output);
        Assertions.assertEquals(
                "usage: " + DocumentDbMain.getLibraryName() + " [-g | -r | -l | -b | -e <[table-name[,...]]> |\n"
                        + "                                 -i <file-name>] -s <host-name> -d\n"
                        + "                                 <database-name> -u <user-name> [-p <password>]\n"
                        + "                                 [-n <schema-name>] [-m <method>] [-x\n"
                        + "                                 <max-documents>] [-t] [-a] [-o <file-name>]\n"
                        + "                                 [-h] [--version]\n"
                        + " -a,--tls-allow-invalid-hostnames  The indicator of whether to allow invalid\n"
                        + "                                   hostnames when connecting to DocumentDB.\n"
                        + "                                   Default: false.\n"
                        + " -b,--list-tables                  Lists the SQL table names in a schema.\n"
                        + " -d,--database <database-name>     The name of the database for the schema\n"
                        + "                                   operations. Required.\n"
                        + " -e,--export <[table-name[,...]]>  Exports the schema to for SQL tables named\n"
                        + "                                   [<table-name>[,<table-name>[…]]]. If no\n"
                        + "                                   <table-name> are given, all table schema will\n"
                        + "                                   be exported. By default, the schema is\n"
                        + "                                   written to stdout. Use the --output option to\n"
                        + "                                   write to a file. The output format is JSON.\n"
                        + " -g,--generate-new                 Generates a new schema for the database. This\n"
                        + "                                   will have the effect of replacing an existing\n"
                        + "                                   schema of the same name, if it exists.\n"
                        + " -h,--help                         Prints the command line syntax.\n"
                        + " -i,--import <file-name>           Imports the schema from <file-name> in your\n"
                        + "                                   home directory. The schema will be imported\n"
                        + "                                   using the <schema-name> and a new version\n"
                        + "                                   will be added - replacing the existing\n"
                        + "                                   schema. The expected input format is JSON.\n"
                        + " -l,--list-schema                  Lists the schema names, version and table\n"
                        + "                                   names available in the schema repository.\n"
                        + " -m,--scan-method <method>         The scan method to sample documents from the\n"
                        + "                                   collections. One of: random, idForward,\n"
                        + "                                   idReverse, or all. Used in conjunction with\n"
                        + "                                   the --generate-new command. Default: random.\n"
                        + " -n,--schema-name <schema-name>    The name of the schema. Default: _default.\n"
                        + " -o,--output <file-name>           Write the exported schema to <file-name> in\n"
                        + "                                   your home directory (instead of stdout). This\n"
                        + "                                   will overwrite any existing file with the\n"
                        + "                                   same name\n"
                        + " -p,--password <password>          The password for the user performing the\n"
                        + "                                   schema operations. Optional. If this option\n"
                        + "                                   is not provided, the end-user will be\n"
                        + "                                   prompted to enter the password directly.\n"
                        + " -r,--remove                       Removes the schema from storage for schema\n"
                        + "                                   given by -m <schema-name>, or for schema\n"
                        + "                                   '_default', if not provided.\n"
                        + " -s,--server <host-name>           The hostname and optional port number\n"
                        + "                                   (default: 27017) in the format\n"
                        + "                                   hostname[:port]. Required.\n"
                        + " -t,--tls                          The indicator of whether to use TLS\n"
                        + "                                   encryption when connecting to DocumentDB.\n"
                        + "                                   Default: false.\n"
                        + " -u,--user <user-name>             The name of the user performing the schema\n"
                        + "                                   operations. Required. Note: the user will\n"
                        + "                                   require readWrite role on the <database-name>\n"
                        + "                                   where the schema are stored if creating or\n"
                        + "                                   modifying schema.\n"
                        + "    --version                      Prints the version number of the command.\n"
                        + " -x,--scan-limit <max-documents>   The maximum number of documents to sample in\n"
                        + "                                   each collection. Used in conjunction with the\n"
                        + "                                   --generate-new command. Default: 1000.\n",
                output.toString().replace("\r\n", "\n"));
    }

    @DisplayName("Tests the version (--version) option")
    @Test
    void testVersionOption() throws SQLException {
        final StringBuilder output = new StringBuilder();
        DocumentDbMain.handleCommandLine(new String[] {"--version"}, output);
        Assertions.assertEquals(String.format(
                "%s: version %s", DocumentDbMain.LIBRARY_NAME, DocumentDbMain.ARCHIVE_VERSION),
                output.toString());
    }

    @ParameterizedTest(name = "testExportFileToDirectoryError - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testExportFileToDirectoryError(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException, IOException {
        setConnectionProperties(testEnvironment);
        final String collectionName = createSimpleCollection(testEnvironment);

        try {
            final String directoryName = UUID.randomUUID().toString().replace("-", "");
            final Path directoryPath = USER_HOME_PATH.resolve(directoryName);
            Files.createDirectory(directoryPath);
            try {
                final StringBuilder output = new StringBuilder();
                final String[] args = buildArguments("-e=" + collectionName, DEFAULT_SCHEMA_NAME,
                        directoryName);
                DocumentDbMain.handleCommandLine(args, output);
                Assertions.assertEquals("Output file name must not be a directory.",
                        output.toString());
            } finally {
                Assertions.assertTrue(directoryPath.toFile().delete());
            }
        } finally {
            dropCollection(testEnvironment, collectionName);
        }
    }

    @ParameterizedTest(name = "testSshTunnelCommand - [{index}] - {arguments}")
    @MethodSource("getDocumentDb40SshTunnelEnvironmentSourceOrNull")
    void testSshTunnelCommand(final DocumentDbTestEnvironment testEnvironment) throws SQLException {
        // NOTE: a "null" environment means it isn't configured to run. So bypass.
        if (testEnvironment == null) {
            return;
        }
        setConnectionProperties(testEnvironment);

        final String connectionString = getSshConnectionString();
        final StringBuilder output = new StringBuilder();
        final String[] args = {"--ssh-tunnel", connectionString};
        DocumentDbMain.handleCommandLine(args, output);
        Assertions.assertEquals("", output.toString());
    }

    private String createSimpleCollection(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        final String collectionName;
        collectionName = testEnvironment.newCollectionName(false);
        createSimpleCollection(testEnvironment, collectionName);
        return collectionName;
    }

    @ParameterizedTest(name = "testExportFileToDirectoryError - [{index}] - {arguments}")
    @MethodSource("getTestEnvironments")
    void testImportFileNotExistsError(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        setConnectionProperties(testEnvironment);
        final String collectionName = createSimpleCollection(testEnvironment);

        try {
            final StringBuilder output = new StringBuilder();
            final String[] args = buildArguments("-i=" + collectionName, DEFAULT_SCHEMA_NAME);
            DocumentDbMain.handleCommandLine(args, output);
            Assertions.assertEquals(
                    String.format("Import file '%s' not found in your user's home folder.",
                            collectionName), output.toString());
        } finally {
            dropCollection(testEnvironment, collectionName);
        }
    }

    private void readOutputFileContent(
            final Path outputFilePath,
            final StringBuilder output) throws IOException {
        try (BufferedReader reader = Files
                .newBufferedReader(outputFilePath, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            boolean isFirst = true;
            while (line != null) {
                if (!isFirst) {
                    output.append(System.lineSeparator());
                }
                isFirst = false;
                output.append(line);
                line = reader.readLine();
            }
        }
    }

    private void setConnectionProperties(final DocumentDbTestEnvironment testEnvironment)
            throws SQLException {
        this.properties = DocumentDbConnectionProperties
                .getPropertiesFromConnectionString(testEnvironment.getJdbcConnectionString());
    }

    private String[] buildArguments(final String command) {
        return buildArguments(command, null);
    }

    private String[] buildArguments(final String command, final String schemaName) {
        return buildArguments(command, schemaName, null, this.properties);
    }

    private String[] buildArguments(final String command, final String schemaName,
            final String outputFileName) {
        return buildArguments(command, schemaName, outputFileName, this.properties);
    }

    private String[] buildArguments(final String command, final String schemaName,
            final String outputFileName, final DocumentDbConnectionProperties properties) {
        final List<String> argsList = new ArrayList<>();
        argsList.add(command);
        argsList.add(String.format("-s=%s", properties.getHostname()));
        argsList.add(String.format("-d=%s", properties.getDatabase()));
        argsList.add(String.format("-u=%s", properties.getUser()));
        argsList.add(String.format("-p=%s", properties.getPassword()));
        if (properties.getTlsEnabled()) {
            argsList.add("-t");
        }
        if (properties.getTlsAllowInvalidHostnames()) {
            argsList.add("-a");
        }
        if (!isNullOrEmpty(schemaName)) {
            argsList.add("-n");
            argsList.add(schemaName);
        }
        if (!isNullOrEmpty(outputFileName)) {
            argsList.add("-o");
            argsList.add(outputFileName);
        }
        return argsList.toArray(new String[0]);
    }

    private void createSimpleCollection(
            final DocumentDbTestEnvironment testEnvironment,
            final String collectionName) throws SQLException {
        try (MongoClient client = testEnvironment.createMongoClient()) {
            final MongoDatabase database = client.getDatabase(testEnvironment.getDatabaseName());
            final MongoCollection<BsonDocument> collection = database
                    .getCollection(collectionName, BsonDocument.class);
            testEnvironment.prepareSimpleConsistentData(collection, 5);
        }
    }

    private void dropCollection(
            final DocumentDbTestEnvironment testEnvironment, final String collectionName)
            throws SQLException {
        try (MongoClient client = testEnvironment.createMongoClient()) {
            final MongoDatabase database = client.getDatabase(testEnvironment.getDatabaseName());
            final MongoCollection<BsonDocument> collection = database
                    .getCollection(collectionName, BsonDocument.class);
            collection.drop();
        }
    }

    private static String getExpectedExportContent(
            final String... collectionNames) {
        if (collectionNames == null || collectionNames.length < 1) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();
        builder.append("[ ");
        boolean isFirst = true;
        for (String collectionName : collectionNames) {
            if (!isFirst) {
                builder.append(", ");
            }
            isFirst = false;
            builder.append("{\n" + "  \"sqlName\" : \"").append(collectionName).append("\",\n")
                    .append("  \"collectionName\" : \"").append(collectionName).append("\",\n")
                    .append("  \"columns\" : [ {\n").append("    \"fieldPath\" : \"_id\",\n")
                    .append("    \"sqlName\" : \"").append(collectionName).append("__id\",\n")
                    .append("    \"sqlType\" : \"varchar\",\n")
                    .append("    \"dbType\" : \"object_id\",\n")
                    .append("    \"isPrimaryKey\" : true\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldDouble\",\n")
                    .append("    \"sqlName\" : \"fieldDouble\",\n")
                    .append("    \"sqlType\" : \"double\",\n")
                    .append("    \"dbType\" : \"double\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldString\",\n")
                    .append("    \"sqlName\" : \"fieldString\",\n")
                    .append("    \"sqlType\" : \"varchar\",\n")
                    .append("    \"dbType\" : \"string\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldObjectId\",\n")
                    .append("    \"sqlName\" : \"fieldObjectId\",\n")
                    .append("    \"sqlType\" : \"varchar\",\n")
                    .append("    \"dbType\" : \"object_id\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldBoolean\",\n")
                    .append("    \"sqlName\" : \"fieldBoolean\",\n")
                    .append("    \"sqlType\" : \"boolean\",\n")
                    .append("    \"dbType\" : \"boolean\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldDate\",\n")
                    .append("    \"sqlName\" : \"fieldDate\",\n")
                    .append("    \"sqlType\" : \"timestamp\",\n")
                    .append("    \"dbType\" : \"date_time\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldInt\",\n")
                    .append("    \"sqlName\" : \"fieldInt\",\n")
                    .append("    \"sqlType\" : \"integer\",\n")
                    .append("    \"dbType\" : \"int32\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldLong\",\n")
                    .append("    \"sqlName\" : \"fieldLong\",\n")
                    .append("    \"sqlType\" : \"bigint\",\n")
                    .append("    \"dbType\" : \"int64\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldMaxKey\",\n")
                    .append("    \"sqlName\" : \"fieldMaxKey\",\n")
                    .append("    \"sqlType\" : \"varchar\",\n")
                    .append("    \"dbType\" : \"max_key\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldMinKey\",\n")
                    .append("    \"sqlName\" : \"fieldMinKey\",\n")
                    .append("    \"sqlType\" : \"varchar\",\n")
                    .append("    \"dbType\" : \"min_key\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldNull\",\n")
                    .append("    \"sqlName\" : \"fieldNull\",\n")
                    .append("    \"sqlType\" : \"null\",\n")
                    .append("    \"dbType\" : \"null\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldBinary\",\n")
                    .append("    \"sqlName\" : \"fieldBinary\",\n")
                    .append("    \"sqlType\" : \"varbinary\",\n")
                    .append("    \"dbType\" : \"binary\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldDecimal128\",\n")
                    .append("    \"sqlName\" : \"fieldDecimal128\",\n")
                    .append("    \"sqlType\" : \"decimal\",\n")
                    .append("    \"dbType\" : \"decimal128\"\n").append("  }, {\n")
                    .append("    \"fieldPath\" : \"fieldTimestamp\",\n")
                    .append("    \"sqlName\" : \"fieldTimestamp\",\n")
                    .append("    \"sqlType\" : \"timestamp\",\n")
                    .append("    \"dbType\" : \"timestamp\"\n")
                    .append("  } ]\n")
                    .append("}");
        }
        builder.append(" ]");
        return builder.toString();
    }

    static String getSshConnectionString() {
        final String docDbRemoteHost = System.getenv(DOC_DB_HOST_PROPERTY);
        final String docDbSshUserAndHost = System.getenv(DOC_DB_USER_PROPERTY);
        final int userSeparatorIndex = docDbSshUserAndHost.indexOf('@');
        final String sshUser = docDbSshUserAndHost.substring(0, userSeparatorIndex);
        final String sshHostname = docDbSshUserAndHost.substring(userSeparatorIndex + 1);
        final String docDbSshPrivKeyFile = System.getenv(DOC_DB_PRIV_KEY_FILE_PROPERTY);
        final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
        properties.setHostname(docDbRemoteHost);
        properties.setSshUser(sshUser);
        properties.setSshHostname(sshHostname);
        properties.setSshPrivateKeyFile(docDbSshPrivKeyFile);
        properties.setSshStrictHostKeyChecking(String.valueOf(false));
        return DocumentDbConnectionProperties.DOCUMENT_DB_SCHEME + properties.buildSshConnectionString();
    }
}
