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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class DocumentDbMain {

    public static final String LIBRARY_NAME;
    public static final String ARCHIVE_VERSION;

    @VisibleForTesting
    static final Options COMPLETE_OPTIONS;
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbMain.class);
    private static final Options HELP_VERSION_OPTIONS;
    private static final Option HELP_OPTION;
    private static final Option VERSION_OPTION;
    private static final OptionGroup COMMAND_OPTIONS;
    private static final List<Option> REQUIRED_OPTIONS;
    private static final List<Option> OPTIONAL_OPTIONS;
    private static final String ARCHIVE_VERSION_DEFAULT = "1.0.0";
    private static final String DATABASE_NAME_ARG_NAME = "database-name";
    private static final String DATABASE_OPTION_FLAG = "d";
    private static final String DATABASE_OPTION_NAME = "database";
    private static final String GENERATE_NAME_OPTION_FLAG = "g";
    private static final String GENERATE_NEW_OPTION_NAME = "generate-new";
    private static final String HELP_OPTION_FLAG = "h";
    private static final String HELP_OPTION_NAME = "help";
    private static final String HOST_NAME_ARG_NAME = "host-name";
    private static final String IMPLEMENTATION_VERSION_ATTR_NAME = "Implementation-Version";
    private static final String LIBRARY_NAME_DEFAULT = "documentdb-jdbc";
    private static final String MANIFEST_MF_RESOURCE_NAME = "META-INF/MANIFEST.MF";
    private static final String MAX_DOCUMENTS_ARG_NAME = "max-documents";
    private static final String METHOD_ARG_NAME = "method";
    private static final String PASSWORD_OPTION_FLAG = "p";
    private static final String PASSWORD_OPTION_NAME = "password";
    private static final String REMOVE_OPTION_FLAG = "r";
    private static final String REMOVE_OPTION_NAME = "remove";
    private static final String SCAN_LIMIT_OPTION_FLAG = "l";
    private static final String SCAN_LIMIT_OPTION_NAME = "scan-limit";
    private static final String SCAN_METHOD_OPTION_FLAG = "m";
    private static final String SCAN_METHOD_OPTION_NAME = "scan-method";
    private static final String SCHEMA_NAME_OPTION_FLAG = "n";
    private static final String SCHEMA_NAME_OPTION_NAME = "schema-name";
    private static final String SERVER_OPTION_FLAG = "s";
    private static final String SERVER_OPTION_NAME = "server";
    private static final String TLS_ALLOW_INVALID_HOSTNAMES_OPTION_FLAG = "a";
    private static final String TLS_ALLOW_INVALID_HOSTNAMES_OPTION_NAME = "tls-allow-invalid-hostnames";
    private static final String TLS_OPTION_FLAG = "t";
    private static final String TLS_OPTION_NAME = "tls";
    private static final String USER_NAME_ARG_NAME = "user-name";
    private static final String USER_OPTION_FLAG = "u";
    private static final String USER_OPTION_NAME = "user";
    private static final String VERSION_OPTION_NAME = "version";
    private static final String GENERATE_NEW_OPTION_DESCRIPTION =
            "Generates a new schema for the database. "
                    + "This will have the effect of replacing an existing schema "
                    + "of the same name, if it exists.";
    private static final String REMOVE_OPTION_DESCRIPTION =
            "Removes the schema from storage for schema given by -m <schema-name>, "
                    + "or for schema '_default', if not provided.";
    private static final String VERSION_OPTION_DESCRIPTION = "Prints the version number of the command.";
    private static final String HELP_OPTION_DESCRIPTION = "Prints the command line syntax.";
    private static final String SERVER_OPTION_DESCRIPTION =
            "The hostname and optional port number (default: 27017) in the format "
                    + "hostname[:port]. Required.";
    private static final String DATABASE_OPTION_DESCRIPTION = "The name of the database for the schema operations. Required.";
    private static final String USER_OPTION_DESCRIPTION =
            "The name of the user performing the schema operations. Required. "
                    + "Note: the user will require readWrite role on the <database-name> where "
                    + "the schema are stored if creating or modifying schema.";
    private static final String PASSWORD_OPTION_DESCRIPTION =
            "The password for the user performing the schema operations. Optional. "
                    + "If this option is not provided, the end-user will be prompted to enter "
                    + "the password directly.";
    private static final String SCHEMA_NAME_OPTION_DESCRIPTION = "The name of the schema. Default: _default.";
    private static final String SCAN_METHOD_OPTION_DESCRIPTION =
            "The scan method to sample documents from the collections. "
                    + "One of: random, idForward, idReverse, or all. "
                    + "Used in conjunction with the --generate-new command. "
                    + "Default: random.";
    private static final String SCAN_LIMIT_OPTION_DESCRIPTION =
            "The maximum number of documents to sample in each collection. "
                    + "Used in conjunction with the --generate-new command. "
                    + "Default: 1000.";
    private static final String TLS_OPTION_DESCRIPTION =
            "The indicator of whether to use TLS encryption when connecting to DocumentDB. "
                    + "Default: false.";
    private static final String TLS_ALLOW_INVALID_HOSTNAMES_OPTION_DESCRIPTION =
            "The indicator of whether to allow invalid hostnames when connecting to "
                    + "DocumentDB. Default: false.";
    private static final String NEW_SCHEMA_VERSION_GENERATED_MESSAGE = "New schema '%s', version '%s' generated.";
    private static final String REMOVED_SCHEMA_MESSAGE = "Removed schema '%s'.";

    static {
        ARCHIVE_VERSION = getArchiveVersion();
        LIBRARY_NAME = getLibraryName();
        HELP_OPTION = buildHelpOption();
        VERSION_OPTION = buildVersionOption();
        COMMAND_OPTIONS = buildCommandOptions();
        REQUIRED_OPTIONS = buildRequiredOptions();
        OPTIONAL_OPTIONS = buildOptionalOptions();

        // Add all option types.
        COMPLETE_OPTIONS = new Options();
        COMPLETE_OPTIONS.addOptionGroup(COMMAND_OPTIONS);
        REQUIRED_OPTIONS.forEach(COMPLETE_OPTIONS::addOption);
        OPTIONAL_OPTIONS.forEach(COMPLETE_OPTIONS::addOption);

        // Add options to check for 'help' or 'version'.
        HELP_VERSION_OPTIONS = new Options()
                .addOption(HELP_OPTION)
                .addOption(VERSION_OPTION);
    }

    /**
     * Performs schema commands via the command line.
     * <pre>
     *  -a,--tls-allow-invalid-hostnames   The indicator of whether to allow
     *                                     invalid hostnames when connecting to
     *                                     DocumentDB. Default: false.
     *  -d,--database &#60;database-name&#62;      The name of the database for the
     *                                     schema operations. Required.
     *  -g,--generate-new                  Generates a new schema for the
     *                                     database. This will have the effect of
     *                                     replacing an existing schema of the
     *                                     same name, if it exists.
     *  -h,--help                          Prints the command line syntax.
     *  -l,--scan-limit &#60;max-documents&#62;    The maximum number of documents to
     *                                     sample in each collection. Used in
     *                                     conjunction with the
     *                                     --generate-new command. Default: 1000.
     *  -m,--scan-method &#60;method&#62;          The scan method to sample documents
     *                                     from the collections. One of: random,
     *                                     idForward, idReverse, or all. Used in
     *                                     conjunction with the
     *                                     --generate-new command. Default:
     *                                     random.
     *  -n,--schema-name &#60;schema-name&#62;     The name of the schema. Default:
     *                                     _default.
     *  -p,--password &#60;password&#62;           The password for the user performing
     *                                     the schema operations. Optional. If
     *                                     this option is not provided, the
     *                                     end-user will be prompted to enter the
     *                                     password directly.
     *  -r,--remove                        Removes the schema from storage for
     *                                     schema given by -m &#60;schema-name&#62;, or
     *                                     for schema '_default', if  not
     *                                     provided.
     *  -s,--server &#60;host-name&#62;            The hostname and optional port number
     *                                     (default: 27017) in the format
     *                                     hostname[:port]. Required.
     *  -t,--tls                           The indicator of whether to use TLS
     *                                     encryption when connecting to
     *                                     DocumentDB. Default: false.
     *  -u,--user &#60;user-name&#62;              The name of the user performing the
     *                                     schema operations. Required. Note: the
     *                                     user will require readWrite role on
     *                                     the &#60;database-name&#62; where the schema
     *                                     are stored if creating or modifying
     *                                     schema.
     *     --version                       Prints the version number of the
     *                                     command."
     * </pre>
     * @param args the command line arguments.
     */
    public static void main(final String[] args) {
        try {
            final StringBuilder output = new StringBuilder();
            handleCommandLine(args, output);
            LOGGER.error("{}", output);
        } catch (ParseException | SQLException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error(
                    "Unexpected exception: '{}'",
                    e.getMessage(),
                    e);
        }
    }

    static void handleCommandLine(final String[] args, final StringBuilder output)
            throws ParseException, SQLException {
        if (handledHelpOrVersionOption(args, output)) {
            return;
        }
        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
            final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
            if (!tryGetConnectionProperties(commandLine, properties, output)) {
                return;
            }
            performCommand(properties, output);
        } catch (Exception e) {
            output.append(e.getMessage());
        }
    }

    private static void performCommand(
            final DocumentDbConnectionProperties properties,
            final StringBuilder output)
            throws SQLException {
        switch (COMMAND_OPTIONS.getSelected()) {
            case GENERATE_NAME_OPTION_FLAG: // --generate-new
                final DocumentDbDatabaseSchemaMetadata schema =  DocumentDbDatabaseSchemaMetadata
                        .get(properties, properties.getSchemaName(), true);
                if (schema != null) {
                    output.append(String.format(NEW_SCHEMA_VERSION_GENERATED_MESSAGE,
                            schema.getSchemaName(),
                            schema.getSchemaVersion()));
                }
                break;
            case REMOVE_OPTION_FLAG: // --remove
                DocumentDbDatabaseSchemaMetadata.remove(properties, properties.getSchemaName());
                output.append(String.format(REMOVED_SCHEMA_MESSAGE, properties.getSchemaName()));
                break;
            default:
                output.append(SqlError.lookup(SqlError.UNSUPPORTED_PROPERTY,
                        COMMAND_OPTIONS.getSelected()));
                break;
        }
    }

    @VisibleForTesting
    static boolean tryGetConnectionProperties(
            final CommandLine commandLine,
            final DocumentDbConnectionProperties properties,
            final StringBuilder output) {
        properties.setHostname(commandLine.getOptionValue(SERVER_OPTION_FLAG));
        properties.setDatabase(commandLine.getOptionValue(DATABASE_OPTION_FLAG));
        properties.setUser(commandLine.getOptionValue(USER_OPTION_FLAG));
        if (commandLine.hasOption(PASSWORD_OPTION_FLAG)) {
            properties.setPassword(commandLine.getOptionValue(PASSWORD_OPTION_FLAG));
        } else {
            // TODO: Refactor resource string lookup
            final String passwordPrompt = SqlError.lookup(SqlError.PASSWORD_PROMPT);
            final Console console = System.console();
            char[] password = null;
            if (console != null) {
                password = console.readPassword(passwordPrompt);
            } else {
                output.append("No console available.");
            }
            if (password == null || password.length == 0) {
                output.append(SqlError.lookup(SqlError.MISSING_PASSWORD));
                return false;
            }
            properties.setPassword(new String(password));
        }
        properties.setTlsEnabled(String.valueOf(commandLine.hasOption(TLS_OPTION_FLAG)));
        properties.setTlsAllowInvalidHostnames(String.valueOf(commandLine.hasOption(TLS_ALLOW_INVALID_HOSTNAMES_OPTION_FLAG)));
        properties.setMetadataScanMethod(commandLine.getOptionValue(
                SCAN_METHOD_OPTION_FLAG,
                DocumentDbConnectionProperty.METADATA_SCAN_METHOD.getDefaultValue()));
        properties.setMetadataScanLimit(commandLine.getOptionValue(
                SCAN_LIMIT_OPTION_FLAG,
                DocumentDbConnectionProperty.METADATA_SCAN_LIMIT.getDefaultValue()));
        properties.setSchemaName(commandLine.getOptionValue(
                SCHEMA_NAME_OPTION_FLAG,
                DocumentDbConnectionProperty.SCHEMA_NAME.getDefaultValue()));
        return true;
    }

    private static boolean handledHelpOrVersionOption(
            final String[] args,
            final StringBuilder output) throws ParseException {
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine = parser.parse(HELP_VERSION_OPTIONS, args, true);
        if (commandLine.hasOption(HELP_OPTION_NAME)) {
            final StringWriter stringWriter = new StringWriter();
            final PrintWriter printWriter = new PrintWriter(stringWriter);
            final HelpFormatter formatter = new HelpFormatter();
            final StringBuilder cmdLineSyntax = new StringBuilder();
            formatCommandLineSyntax(cmdLineSyntax);
            formatter.printHelp(printWriter,
                    80,
                    cmdLineSyntax.toString(),
                    null,
                    COMPLETE_OPTIONS,
                    1,
                    2,
                    null,
                    false);
            output.append(stringWriter);
            return true;
        } else if (commandLine.hasOption(VERSION_OPTION_NAME)) {
            output.append(String.format("%s: version %s", LIBRARY_NAME, ARCHIVE_VERSION));
            return true;
        }
        return false;
    }

    private static void formatCommandLineSyntax(final StringBuilder cmdLineSyntax) {
        cmdLineSyntax.append(LIBRARY_NAME);
        formatOptionGroup(cmdLineSyntax);
        formatOptions(cmdLineSyntax, REQUIRED_OPTIONS);
        formatOptions(cmdLineSyntax, OPTIONAL_OPTIONS);
    }

    private static void formatOptions(
            final StringBuilder cmdLineSyntax,
            final Collection<Option> options) {
        for (Option option : options) {
            cmdLineSyntax.append(" ");
            if (!option.isRequired()) {
                cmdLineSyntax.append("[");
            }
            if (option.getOpt() != null) {
                cmdLineSyntax.append("-").append(option.getOpt());
            } else {
                cmdLineSyntax.append("--").append(option.getLongOpt());
            }
            if (option.hasArg()) {
                cmdLineSyntax.append(String.format(" <%s>", option.getArgName()));
            } else if (option.hasOptionalArg()) {
                cmdLineSyntax.append(String.format(" [<%s>]", option.getArgName()));
            }
            if (!option.isRequired()) {
                cmdLineSyntax.append("]");
            }
        }
    }

    private static void formatOptionGroup(
            final StringBuilder cmdLineSyntax) {
        boolean isFirst = true;
        for (Option option : COMMAND_OPTIONS.getOptions()) {
            if (isFirst) {
                cmdLineSyntax.append(" ");
            } else {
                cmdLineSyntax.append(" | ");
            }
            if (!COMMAND_OPTIONS.isRequired()) {
                cmdLineSyntax.append("[");
            }
            if (option.getOpt() != null) {
                cmdLineSyntax.append("-").append(option.getOpt());
            } else {
                cmdLineSyntax.append("--").append(option.getLongOpt());
            }
            if (option.hasArg()) {
                cmdLineSyntax.append(String.format(" <%s>", option.getArgName()));
            } else if (option.hasOptionalArg()) {
                cmdLineSyntax.append(String.format(" [<%s>]", option.getArgName()));
            }
            if (!COMMAND_OPTIONS.isRequired()) {
                cmdLineSyntax.append("]");
            }
            isFirst = false;
        }
    }

    private static List<Option> buildOptionalOptions() {
        final List<Option> optionalOptions = new ArrayList<>();
        Option currOption;
        currOption = Option.builder(PASSWORD_OPTION_FLAG)
                .longOpt(PASSWORD_OPTION_NAME)
                .numberOfArgs(1)
                .argName(PASSWORD_OPTION_NAME)
                .desc(PASSWORD_OPTION_DESCRIPTION)
                .required(false)
                .build();
        optionalOptions.add(currOption);
        currOption = Option.builder(SCHEMA_NAME_OPTION_FLAG)
                .longOpt(SCHEMA_NAME_OPTION_NAME)
                .numberOfArgs(1)
                .argName(SCHEMA_NAME_OPTION_NAME)
                .desc(SCHEMA_NAME_OPTION_DESCRIPTION)
                .required(false)
                .build();
        optionalOptions.add(currOption);
        currOption = Option.builder(SCAN_METHOD_OPTION_FLAG)
                .longOpt(SCAN_METHOD_OPTION_NAME)
                .numberOfArgs(1)
                .argName(METHOD_ARG_NAME)
                .desc(SCAN_METHOD_OPTION_DESCRIPTION)
                .required(false)
                .type(DocumentDbMetadataScanMethod.class)
                .build();
        optionalOptions.add(currOption);
        currOption = Option.builder(SCAN_LIMIT_OPTION_FLAG)
                .longOpt(SCAN_LIMIT_OPTION_NAME)
                .numberOfArgs(1)
                .argName(MAX_DOCUMENTS_ARG_NAME)
                .desc(SCAN_LIMIT_OPTION_DESCRIPTION)
                .required(false)
                .type(Integer.class)
                .build();
        optionalOptions.add(currOption);
        currOption = Option.builder(TLS_OPTION_FLAG)
                .longOpt(TLS_OPTION_NAME)
                .desc(TLS_OPTION_DESCRIPTION)
                .required(false)
                .build();
        optionalOptions.add(currOption);
        currOption = Option.builder(TLS_ALLOW_INVALID_HOSTNAMES_OPTION_FLAG)
                .longOpt(TLS_ALLOW_INVALID_HOSTNAMES_OPTION_NAME)
                .desc(TLS_ALLOW_INVALID_HOSTNAMES_OPTION_DESCRIPTION)
                .required(false)
                .build();
        optionalOptions.add(currOption);
        optionalOptions.add(HELP_OPTION);
        optionalOptions.add(VERSION_OPTION);

        return optionalOptions;
    }

    private static List<Option> buildRequiredOptions() {
        final List<Option> requiredOptions = new ArrayList<>();
        Option currOption;
        currOption = Option.builder(SERVER_OPTION_FLAG)
                .longOpt(SERVER_OPTION_NAME)
                .numberOfArgs(1)
                .argName(HOST_NAME_ARG_NAME)
                .desc(SERVER_OPTION_DESCRIPTION)
                .required()
                .build();
        requiredOptions.add(currOption);
        currOption = Option.builder(DATABASE_OPTION_FLAG)
                .longOpt(DATABASE_OPTION_NAME)
                .numberOfArgs(1)
                .argName(DATABASE_NAME_ARG_NAME)
                .desc(DATABASE_OPTION_DESCRIPTION)
                .required()
                .build();
        requiredOptions.add(currOption);
        currOption = Option.builder(USER_OPTION_FLAG)
                .longOpt(USER_OPTION_NAME)
                .numberOfArgs(1)
                .argName(USER_NAME_ARG_NAME)
                .desc(USER_OPTION_DESCRIPTION)
                .required()
                .build();
        requiredOptions.add(currOption);

        return requiredOptions;
    }

    private static OptionGroup buildCommandOptions() {
        final OptionGroup commandOptions = new OptionGroup();
        Option currOption;
        currOption = Option.builder(GENERATE_NAME_OPTION_FLAG)
                .longOpt(GENERATE_NEW_OPTION_NAME)
                .desc(GENERATE_NEW_OPTION_DESCRIPTION)
                .build();
        commandOptions.addOption(currOption);
        currOption = Option.builder(REMOVE_OPTION_FLAG)
                .longOpt(REMOVE_OPTION_NAME)
                .desc(REMOVE_OPTION_DESCRIPTION)
                .build();
        commandOptions.addOption(currOption);
        commandOptions.setRequired(true);

        return commandOptions;
    }

    private static String getLibraryName() {
        String libraryName = null;
        try {
            final Path path = Paths.get(DocumentDbMain.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI());
            final Path fileName = path.getFileName();
            if (fileName != null) {
                libraryName = fileName.toString();
            } else {
                libraryName = LIBRARY_NAME_DEFAULT;
            }
        } catch (URISyntaxException e) {
            libraryName = LIBRARY_NAME_DEFAULT;
        } finally {
            if (libraryName == null) {
                libraryName = LIBRARY_NAME_DEFAULT;
            }
        }
        return libraryName;
    }

    private static String getArchiveVersion() {
        final String archiveVersion;
        final URLClassLoader cl = (URLClassLoader) DocumentDbMain.class.getClassLoader();
        final InputStream resource = cl.getResourceAsStream(MANIFEST_MF_RESOURCE_NAME);
        Manifest manifest = null;
        try {
            manifest = new Manifest(resource);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        if (manifest != null) {
            final Attributes attributes = manifest.getMainAttributes();
            if (attributes != null) {
                archiveVersion = attributes.getValue(IMPLEMENTATION_VERSION_ATTR_NAME);
            } else {
                archiveVersion = ARCHIVE_VERSION_DEFAULT;
            }
        } else {
            archiveVersion = ARCHIVE_VERSION_DEFAULT;
        }
        return archiveVersion;
    }

    private static Option buildVersionOption() {
        return Option.builder()
                .longOpt(VERSION_OPTION_NAME)
                .desc(VERSION_OPTION_DESCRIPTION)
                .build();
    }

    private static Option buildHelpOption() {
        return Option.builder(HELP_OPTION_FLAG)
                .longOpt(HELP_OPTION_NAME)
                .desc(HELP_OPTION_DESCRIPTION)
                .build();
    }
}
