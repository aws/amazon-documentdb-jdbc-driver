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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.mongodb.DuplicateKeyException;
import com.mongodb.client.MongoClient;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.documentdb.jdbc.common.utilities.SqlError;
import software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchema;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaColumn;
import software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable;
import software.amazon.documentdb.jdbc.persist.DocumentDbSchemaSecurityException;
import software.amazon.documentdb.jdbc.sshtunnel.DocumentDbSshTunnelService;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static software.amazon.documentdb.jdbc.DocumentDbConnectionProperties.USER_HOME_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_LATEST_OR_NONE;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbDatabaseSchemaMetadata.VERSION_NEW;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchema.SQL_NAME_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable.COLLECTION_NAME_PROPERTY;
import static software.amazon.documentdb.jdbc.metadata.DocumentDbSchemaTable.COLUMNS_PROPERTY;

public class DocumentDbMain {

    public static final String LIBRARY_NAME;
    public static final String ARCHIVE_VERSION;
    public static final Path USER_HOME_PATH = Paths.get(System.getProperty(USER_HOME_PROPERTY));

    @VisibleForTesting
    static final Options COMPLETE_OPTIONS;
    @VisibleForTesting
    static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX";

    static final ObjectMapper JSON_OBJECT_MAPPER = JsonMapper.builder()
            .defaultDateFormat(new StdDateFormat().withColonInTimeZone(true))
            .serializationInclusion(Include.NON_NULL)
            .serializationInclusion(Include.NON_EMPTY)
            .serializationInclusion(Include.NON_DEFAULT)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            // Make the enums serialize to lower case.
            .addModule(buildEnumLowerCaseSerializerModule())
            .addModule(new GuavaModule()) // Immutable*
            .build();
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDbMain.class);
    private static final Options HELP_VERSION_OPTIONS;
    private static final Option HELP_OPTION;
    private static final Option VERSION_OPTION;
    private static final Options SSH_TUNNEL_SERVICE_OPTIONS;
    private static final OptionGroup COMMAND_OPTIONS;
    private static final List<Option> REQUIRED_OPTIONS;
    private static final List<Option> OPTIONAL_OPTIONS;
    // String constants
    private static final String LIBRARY_NAME_DEFAULT = "documentdb-jdbc";

    // Option string constants
    private static final String DATABASE_OPTION_FLAG = "d";
    private static final String DATABASE_OPTION_NAME = "database";
    private static final String EXPORT_OPTION_FLAG = "e";
    private static final String EXPORT_OPTION_NAME = "export";
    private static final String GENERATE_NAME_OPTION_FLAG = "g";
    private static final String GENERATE_NEW_OPTION_NAME = "generate-new";
    private static final String HELP_OPTION_FLAG = "h";
    private static final String HELP_OPTION_NAME = "help";
    private static final String IMPORT_OPTION_FLAG = "i";
    private static final String IMPORT_OPTION_NAME = "import";
    private static final String LIST_OPTION_FLAG = "l";
    private static final String LIST_OPTION_NAME = "list-schema";
    private static final String LIST_TABLES_OPTION_FLAG = "b";
    private static final String LIST_TABLES_OPTION_NAME = "list-tables";
    private static final String OUTPUT_OPTION_FLAG = "o";
    private static final String OUTPUT_OPTION_NAME = "output";
    private static final String PASSWORD_OPTION_FLAG = "p";
    private static final String PASSWORD_OPTION_NAME = "password";
    private static final String REMOVE_OPTION_FLAG = "r";
    private static final String REMOVE_OPTION_NAME = "remove";
    private static final String SCAN_LIMIT_OPTION_FLAG = "x";
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
    private static final String USER_OPTION_FLAG = "u";
    private static final String USER_OPTION_NAME = "user";
    private static final String VERSION_OPTION_NAME = "version";
    public static final String SSH_TUNNEL_SERVICE_OPTION_NAME = "ssh-tunnel-service";
    // Option argument string constants
    private static final String DATABASE_NAME_ARG_NAME = "database-name";
    private static final String FILE_NAME_ARG_NAME = "file-name";
    private static final String HOST_NAME_ARG_NAME = "host-name";
    private static final String MAX_DOCUMENTS_ARG_NAME = "max-documents";
    private static final String METHOD_ARG_NAME = "method";
    private static final String USER_NAME_ARG_NAME = "user-name";
    private static final String TABLE_NAMES_ARG_NAME = "[table-name[,...]]";
    private static final String SSH_TUNNEL_SERVICE_ARG_NAME = "ssh-properties";
    // Option description string constants
    private static final String GENERATE_NEW_OPTION_DESCRIPTION =
            "Generates a new schema for the database. "
                    + "This will have the effect of replacing an existing schema "
                    + "of the same name, if it exists.";
    private static final String REMOVE_OPTION_DESCRIPTION =
            "Removes the schema from storage for schema given by -m <schema-name>, "
                    + "or for schema '_default', if not provided.";
    private static final String VERSION_OPTION_DESCRIPTION =
            "Prints the version number of the"
                    + " command.";
    private static final String HELP_OPTION_DESCRIPTION =
            "Prints the command line syntax.";
    private static final String SERVER_OPTION_DESCRIPTION =
            "The hostname and optional port number (default: 27017) in the format "
                    + "hostname[:port]. Required.";
    private static final String DATABASE_OPTION_DESCRIPTION =
            "The name of the database for the schema operations. Required.";
    private static final String USER_OPTION_DESCRIPTION =
            "The name of the user performing the schema operations. Required. "
                    + "Note: the user will require readWrite role on the <database-name> where "
                    + "the schema are stored if creating or modifying schema.";
    private static final String PASSWORD_OPTION_DESCRIPTION =
            "The password for the user performing the schema operations. Optional. "
                    + "If this option is not provided, the end-user will be prompted to enter "
                    + "the password directly.";
    private static final String SCHEMA_NAME_OPTION_DESCRIPTION =
            "The name of the schema. Default: _default.";
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
    private static final String LIST_OPTION_DESCRIPTION =
            "Lists the schema names, version and table names available in the schema repository.";
    private static final String LIST_TABLES_OPTION_DESCRIPTION =
            "Lists the SQL table names in a schema.";
    private static final String EXPORT_OPTION_DESCRIPTION =
            "Exports the schema to for SQL tables named [<table-name>[,<table-name>[…]]]. If no"
                    + " <table-name> are given, all table schema will be exported. By default,"
                    + " the schema is written to stdout. Use the --output option to write to a file."
                    + " The output format is JSON.";
    private static final String IMPORT_OPTION_DESCRIPTION =
            "Imports the schema from <file-name> in your home directory. The schema will be imported using the"
                    + " <schema-name> and a new version will be added - replacing the existing"
                    + " schema. The expected input format is JSON.";
    private static final String OUTPUT_OPTION_DESCRIPTION =
            "Write the exported schema to <file-name> in your home directory (instead of stdout)."
                    + " This will overwrite any existing file with the same name";
    private static final String SSH_TUNNEL_SERVICE_OPTION_DESCRIPTION =
            "Starts an SSH Tunnel service.";
    // Messages string constants
    public static final String DUPLICATE_COLUMN_KEY_DETECTED_FOR_TABLE_SCHEMA =
            "Duplicate column key '%s' detected for table schema '%s'. Original column '%s'."
                    + " Duplicate column '%s'.";
    private static final String NEW_SCHEMA_VERSION_GENERATED_MESSAGE =
            "New schema '%s', version '%s' generated.";
    private static final String REMOVED_SCHEMA_MESSAGE = "Removed schema '%s'.";

    private static MongoClient client;

    static {
        ARCHIVE_VERSION = getArchiveVersion();
        LIBRARY_NAME = getLibraryName();
        HELP_OPTION = buildHelpOption();
        VERSION_OPTION = buildVersionOption();
        SSH_TUNNEL_SERVICE_OPTIONS = buildSshTunnelServiceOption();
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
     * -a,--tls-allow-invalid-hostnames  The indicator of whether to allow invalid
     *                                   hostnames when connecting to DocumentDB.
     *                                   Default: false.
     * -b,--list-tables                  Lists the SQL table names in a schema.
     * -d,--database &#60;database-name&#62;     The name of the database for the schema
     *                                   operations. Required.
     * -e,--export &#60;[table-name[,...]]&#62;  Exports the schema to for SQL tables named
     *                                   [&#60;table-name&#62;[,&#60;table-name&#62;[…]]]. If no
     *                                   &#60;table-name&#62; are given, all table schema will
     *                                   be exported. By default, the schema is
     *                                   written to stdout. Use the --output option to
     *                                   write to a file. The output format is JSON.
     * -g,--generate-new                 Generates a new schema for the database. This
     *                                   will have the effect of replacing an existing
     *                                   schema of the same name, if it exists.
     * -h,--help                         Prints the command line syntax.
     * -i,--import &#60;file-name&#62;           Imports the schema from &#60;file-name&#62; in your
     *                                   home directory. The schema will be imported
     *                                   using the &#60;schema-name&#62; and a new version
     *                                   will be added - replacing the existing
     *                                   schema. The expected input format is JSON.
     * -l,--list-schema                  Lists the schema names, version and table
     *                                   names available in the schema repository.
     * -m,--scan-method &#60;method&#62;         The scan method to sample documents from the
     *                                   collections. One of: random, idForward,
     *                                   idReverse, or all. Used in conjunction with
     *                                   the --generate-new command. Default: random.
     * -n,--schema-name &#60;schema-name&#62;    The name of the schema. Default: _default.
     * -o,--output &#60;file-name&#62;           Write the exported schema to &#60;file-name&#62; in
     *                                   your home directory (instead of stdout). This
     *                                   will overwrite any existing file with the
     *                                   same name
     * -p,--password &#60;password&#62;          The password for the user performing the
     *                                   schema operations. Optional. If this option
     *                                   is not provided, the end-user will be
     *                                   prompted to enter the password directly.
     * -r,--remove                       Removes the schema from storage for schema
     *                                   given by -m &#60;schema-name&#62;, or for schema
     *                                   '_default', if not provided.
     * -s,--server &#60;host-name&#62;           The hostname and optional port number
     *                                   (default: 27017) in the format
     *                                   hostname[:port]. Required.
     * -t,--tls                          The indicator of whether to use TLS
     *                                   encryption when connecting to DocumentDB.
     *                                   Default: false.
     * -u,--user &#60;user-name&#62;             The name of the user performing the schema
     *                                   operations. Required. Note: the user will
     *                                   require readWrite role on the &#60;database-name&#62;
     *                                   where the schema are stored if creating or
     *                                   modifying schema.
     *    --version                      Prints the version number of the command.
     * -x,--scan-limit &#60;max-documents&#62;   The maximum number of documents to sample in
     *                                   each collection. Used in conjunction with the
     *                                   --generate-new command. Default: 1000.,
     * </pre>
     * @param args the command line arguments.
     */
    public static void main(final String[] args) {
        try {
            final StringBuilder output = new StringBuilder();
            handleCommandLine(args, output);
            if (output.length() > 0) {
                LOGGER.error("{}", output);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.error(
                    "Unexpected exception: '{}'",
                    e.getMessage(),
                    e);
        }
    }

    static void handleCommandLine(final String[] args, final StringBuilder output)
            throws SQLException {
        if (handledHelpOrVersionOption(args, output)) {
            return;
        }
        try {
            final CommandLineParser parser = new DefaultParser();
            // First check for the SSH tunnel service option separately from the other options.
            final CommandLine commandLineSshTunnelService = parser.parse(SSH_TUNNEL_SERVICE_OPTIONS, args, true);
            if (commandLineSshTunnelService.hasOption(SSH_TUNNEL_SERVICE_OPTION_NAME)) {
                performSshTunnelService(commandLineSshTunnelService, output);
                return;
            }
            // Otherwise, consider the "complete" options for metadata options.
            final CommandLine commandLine = parser.parse(COMPLETE_OPTIONS, args);
            final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
            if (!tryGetConnectionProperties(commandLine, properties, output)) {
                return;
            }
            performCommand(commandLine, properties, output);
        } catch (MissingOptionException e) {
            output.append(e.getMessage()).append(String.format("%n"));
            printHelp(output);
        } catch (ParseException e) {
            output.append(e.getMessage());
        } catch (Exception e) {
            output.append(e.getClass().getSimpleName())
                    .append(": ")
                    .append(e.getMessage());
        } finally {
            closeClient();
        }
    }

    private static void performCommand(
            final CommandLine commandLine,
            final DocumentDbConnectionProperties properties,
            final StringBuilder output)
            throws SQLException {
        switch (COMMAND_OPTIONS.getSelected()) {
            case GENERATE_NAME_OPTION_FLAG: // --generate-new
                performGenerateNew(properties, output);
                break;
            case REMOVE_OPTION_FLAG: // --remove
                performRemove(properties, output);
                break;
            case LIST_OPTION_FLAG: // --list-schema
                performListSchema(properties, output);
                break;
            case LIST_TABLES_OPTION_FLAG: // --list-tables
                performListTables(properties, output);
                break;
            case EXPORT_OPTION_FLAG: // --export
                performExport(commandLine, properties, output);
                break;
            case IMPORT_OPTION_FLAG: // --import
                performImport(commandLine, properties, output);
                break;
            default:
                output.append(SqlError.lookup(SqlError.UNSUPPORTED_PROPERTY,
                        COMMAND_OPTIONS.getSelected()));
                break;
        }
    }

    private static MongoClient getMongoClient(final DocumentDbConnectionProperties properties) {
        if (client == null) {
            client = properties.createMongoClient();
        }
        return client;
    }

    private static void closeClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private static void performSshTunnelService(
            final CommandLine commandLine,
            final StringBuilder output) throws DuplicateKeyException {
        try (DocumentDbSshTunnelService service = new DocumentDbSshTunnelService(
                    commandLine.getOptionValue(SSH_TUNNEL_SERVICE_OPTION_NAME))) {
            final Thread serviceThread = new Thread(service);
            serviceThread.setDaemon(true);
            serviceThread.start();
            do {
                serviceThread.join(1000);
            } while (serviceThread.isAlive());
            service.getExceptions().forEach(
                    e -> output
                            .append(e.getMessage())
                            .append(System.lineSeparator())
                            .append(Arrays.stream(e.getStackTrace())
                                    .map(StackTraceElement::toString)
                                    .collect(Collectors.joining(System.lineSeparator())))
                            .append(System.lineSeparator()));
        } catch (Exception e) {
            output.append(e.getMessage());
        }
    }

    private static void performImport(
            final CommandLine commandLine,
            final DocumentDbConnectionProperties properties,
            final StringBuilder output) throws DuplicateKeyException {
        final File importFile = tryGetImportFile(commandLine, output);
        if (importFile == null) {
            return;
        }

        final List<TableSchema> tableSchemaList = tryReadTableSchemaList(importFile, output);
        if (tableSchemaList == null) {
            return;
        }

        final List<DocumentDbSchemaTable> schemaTableList = tryGetSchemaTableList(
                tableSchemaList, output);
        if (schemaTableList == null) {
            return;
        }

        updateTableSchema(properties, schemaTableList, output);
    }

    private static void updateTableSchema(
            final DocumentDbConnectionProperties properties,
            final List<DocumentDbSchemaTable> schemaTableList,
            final StringBuilder output) {
        try {
            DocumentDbDatabaseSchemaMetadata.update(
                    properties,
                    properties.getSchemaName(),
                    schemaTableList,
                    getMongoClient(properties));
        } catch (SQLException | DocumentDbSchemaSecurityException e) {
            output.append(e.getClass().getSimpleName())
                    .append(" ")
                    .append(e.getMessage());
        }
    }

    private static List<TableSchema> tryReadTableSchemaList(
            final File importFile,
            final StringBuilder output) {
        final List<TableSchema> tableSchemaList;
        try {
            tableSchemaList = JSON_OBJECT_MAPPER.readValue(importFile,
                    new TypeReference<List<TableSchema>>() { });
        } catch (IOException e) {
            output.append(e.getClass().getSimpleName())
                    .append(" ")
                    .append(e.getMessage());
            return null;
        }
        return tableSchemaList;
    }

    private static List<DocumentDbSchemaTable> tryGetSchemaTableList(
            final List<TableSchema> tableSchemaList, final StringBuilder output) {
        final List<DocumentDbSchemaTable> schemaTableList;
        try {
            schemaTableList = tableSchemaList.stream()
                    .map(tableSchema -> new DocumentDbSchemaTable(
                            tableSchema.getSqlName(),
                            tableSchema.getCollectionName(),
                            tableSchema.getColumns().stream()
                                    .collect(Collectors.toMap(
                                            DocumentDbSchemaColumn::getSqlName,
                                            c -> c,
                                            (c1, c2) -> throwingDuplicateMergeOnColumn(c1, c2,
                                                    tableSchema.getSqlName()),
                                            LinkedHashMap::new))))
                    .collect(Collectors.toList());
        } catch (IllegalStateException e) {
            output.append(e.getMessage());
            return  null;
        }
        return schemaTableList;
    }

    private static DocumentDbSchemaColumn throwingDuplicateMergeOnColumn(
            final DocumentDbSchemaColumn c1,
            final DocumentDbSchemaColumn c2,
            final String sqlName) {
        throw new IllegalStateException(String.format(DUPLICATE_COLUMN_KEY_DETECTED_FOR_TABLE_SCHEMA,
                c1.getSqlName(),
                sqlName,
                c1,
                c2));
    }

    private static File tryGetImportFile(
            final CommandLine commandLine,
            final StringBuilder output) {
        final String importFileName = commandLine.getOptionValue(IMPORT_OPTION_FLAG, null);
        if (isNullOrEmpty(importFileName)) {
            output.append(String.format("Option '-%s' requires a file name argument.", IMPORT_OPTION_FLAG));
            return null;
        }
        final Path importFilePath = USER_HOME_PATH.resolve(importFileName);
        if (!importFilePath.toFile().exists()) {
            output.append(String.format("Import file '%s' not found in your user's home folder.", importFileName));
            return null;
        }
        return importFilePath.toFile();
    }

    private static void performExport(
            final CommandLine commandLine,
            final DocumentDbConnectionProperties properties,
            final StringBuilder output) throws SQLException {

        // Determine if output file is required.
        final File outputFile;
        if (commandLine.hasOption(OUTPUT_OPTION_FLAG)) {
            outputFile = tryGetOutputFile(commandLine, output);
            if (outputFile == null) {
                return;
            }
        } else {
            outputFile = null;
        }

        final String[] requestedTableNames = commandLine.getOptionValues(EXPORT_OPTION_FLAG);
        final List<String> requestedTableList = requestedTableNames != null
                ? Arrays.asList(requestedTableNames)
                : new ArrayList<>();
        final DocumentDbDatabaseSchemaMetadata schema = DocumentDbDatabaseSchemaMetadata.get(
                properties,
                properties.getSchemaName(),
                VERSION_LATEST_OR_NONE,
                getMongoClient(properties));
        if (schema == null) {
            // No schema to export.
            return;
        }
        final Set<String> availTableSet = schema.getTableSchemaMap().keySet();
        if (requestedTableList.isEmpty()) {
            requestedTableList.addAll(availTableSet);
        } else if (verifyRequestedTablesExist(requestedTableList, availTableSet, output)) {
            return;
        }
        final List<TableSchema> tableSchemaList = requestedTableList.stream()
                .map(tableName -> new TableSchema(schema.getTableSchemaMap().get(tableName)))
                .sorted(Comparator.comparing(TableSchema::getSqlName))
                .collect(Collectors.toList());
        try {
            writeTableSchemas(tableSchemaList, outputFile, output);
        } catch (IOException e) {
            output.append(e.getClass().getSimpleName())
                    .append(" ")
                    .append(e.getMessage());
        }
    }

    private static boolean verifyRequestedTablesExist(
            final List<String> requestedTableList,
            final Set<String> availTableNames,
            final StringBuilder output) {
        if (!availTableNames.containsAll(requestedTableList)) {
            final List<String> unknownTables = requestedTableList.stream()
                    .filter(name -> !availTableNames.contains(name))
                    .collect(Collectors.toList());
            output.append("Requested table name(s) are not recognized in schema: ")
                    .append(Strings.join(unknownTables, ','))
                    .append(String.format("%n"))
                    .append("Available table names: ")
                    .append(Strings.join(availTableNames, ','));
            return true;
        }
        return false;
    }

    private static void writeTableSchemas(
            final List<TableSchema> tables, final File outputFile, final StringBuilder output) throws IOException {
        try (Writer writer = outputFile != null
                ? new OutputStreamWriter(Files.newOutputStream(outputFile.toPath()), StandardCharsets.UTF_8)
                : new StringBuilderWriter(output)) {
            JSON_OBJECT_MAPPER.writeValue(writer, tables);
        }
    }

    private static File tryGetOutputFile(final CommandLine commandLine, final StringBuilder output) {
        if (!USER_HOME_PATH.toFile().exists()) {
            output.append("User's home directory does not exist.");
            return null;
        }
        final String outputFileName = commandLine.getOptionValue(OUTPUT_OPTION_FLAG, null);
        if (isNullOrEmpty(outputFileName)) {
            output.append("Output file name argument must not be empty.");
            return null;
        }
        final Path fileNamePath = Paths.get(outputFileName).getFileName();
        final File outputFile =  USER_HOME_PATH.resolve(fileNamePath).toAbsolutePath().toFile();
        if (outputFile.isDirectory()) {
            output.append("Output file name must not be a directory.");
            return null;
        }
        return outputFile;
    }

    private static void performListSchema(
            final DocumentDbConnectionProperties properties,
            final StringBuilder output) throws SQLException {
        final List<DocumentDbSchema> schemas = DocumentDbDatabaseSchemaMetadata.getSchemaList(
                properties, getMongoClient(properties));
        for (DocumentDbSchema schema : schemas) {
            output.append(String.format("Name=%s, Version=%d, SQL Name=%s, Modified=%s%n",
                    maybeQuote(schema.getSchemaName()),
                    schema.getSchemaVersion(),
                    maybeQuote(schema.getSqlName()),
                    new SimpleDateFormat(DATE_FORMAT_PATTERN)
                            .format(schema.getModifyDate()))
            );
        }
    }

    private static void performListTables(
            final DocumentDbConnectionProperties properties,
            final StringBuilder output) throws SQLException {
        final DocumentDbDatabaseSchemaMetadata schema = DocumentDbDatabaseSchemaMetadata.get(
                properties,
                properties.getSchemaName(),
                VERSION_LATEST_OR_NONE,
                getMongoClient(properties));
        if (schema != null) {
            final List<String> sortedTableNames = schema.getTableSchemaMap().keySet().stream()
                    .sorted()
                    .collect(Collectors.toList());
            for (String tableName : sortedTableNames) {
                output.append(String.format("%s%n", tableName));
            }
        }
    }

    @VisibleForTesting
    static String maybeQuote(final String value) {
        return StringEscapeUtils.escapeCsv(value);
    }

    private static void performRemove(
            final DocumentDbConnectionProperties properties,
            final StringBuilder output) throws SQLException {
        DocumentDbDatabaseSchemaMetadata.remove(
                properties,
                properties.getSchemaName(),
                getMongoClient(properties));
        output.append(String.format(REMOVED_SCHEMA_MESSAGE, properties.getSchemaName()));
    }

    private static void performGenerateNew(
            final DocumentDbConnectionProperties properties,
            final StringBuilder output) throws SQLException {
        final DocumentDbDatabaseSchemaMetadata schema =  DocumentDbDatabaseSchemaMetadata.get(
                properties,
                properties.getSchemaName(),
                VERSION_NEW,
                getMongoClient(properties));
        if (schema != null) {
            output.append(String.format(NEW_SCHEMA_VERSION_GENERATED_MESSAGE,
                    schema.getSchemaName(),
                    schema.getSchemaVersion()));
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
        if (!trySetPassword(commandLine, properties, output)) {
            return false;
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

    private static boolean trySetPassword(final CommandLine commandLine,
            final DocumentDbConnectionProperties properties, final StringBuilder output) {
        if (commandLine.hasOption(PASSWORD_OPTION_FLAG)) {
            properties.setPassword(commandLine.getOptionValue(PASSWORD_OPTION_FLAG));
        } else {
            return trySetPasswordFromPromptInput(properties, output);
        }
        return true;
    }

    private static boolean trySetPasswordFromPromptInput(
            final DocumentDbConnectionProperties properties,
            final StringBuilder output) {
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
        return true;
    }

    private static boolean handledHelpOrVersionOption(
            final String[] args,
            final StringBuilder output) throws SQLException {
        final CommandLineParser parser = new DefaultParser();
        final CommandLine commandLine;
        try {
            commandLine = parser.parse(HELP_VERSION_OPTIONS, args, true);
        } catch (ParseException e) {
            throw new SQLException(e.getMessage(), e);
        }
        if (commandLine.hasOption(HELP_OPTION_NAME)) {
            printHelp(output);
            return true;
        } else if (commandLine.hasOption(VERSION_OPTION_NAME)) {
            output.append(String.format("%s: version %s", LIBRARY_NAME, ARCHIVE_VERSION));
            return true;
        }
        return false;
    }

    private static void printHelp(final StringBuilder output) {
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(stringWriter);
        final HelpFormatter formatter = new HelpFormatter();
        final String cmdLineSyntax = formatCommandLineSyntax();
        formatter.printHelp(printWriter,
                80,
                cmdLineSyntax,
                null,
                COMPLETE_OPTIONS,
                1,
                2,
                null,
                false);
        output.append(stringWriter);
    }

    private static String formatCommandLineSyntax() {
        final StringBuilder cmdLineSyntax = new StringBuilder();
        cmdLineSyntax.append(LIBRARY_NAME);
        formatOptionGroup(cmdLineSyntax);
        formatOptions(cmdLineSyntax, REQUIRED_OPTIONS);
        formatOptions(cmdLineSyntax, OPTIONAL_OPTIONS);
        return cmdLineSyntax.toString();
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
        cmdLineSyntax.append(" [");
        boolean isFirst = true;
        for (Option option : COMMAND_OPTIONS.getOptions()) {
            if (!isFirst) {
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
        cmdLineSyntax.append("]");
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
        currOption = Option.builder(OUTPUT_OPTION_FLAG)
                .longOpt(OUTPUT_OPTION_NAME)
                .desc(OUTPUT_OPTION_DESCRIPTION)
                .numberOfArgs(1)
                .argName(FILE_NAME_ARG_NAME)
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
        currOption = Option.builder(LIST_OPTION_FLAG)
                .longOpt(LIST_OPTION_NAME)
                .desc(LIST_OPTION_DESCRIPTION)
                .build();
        commandOptions.addOption(currOption);
        currOption = Option.builder(LIST_TABLES_OPTION_FLAG)
                .longOpt(LIST_TABLES_OPTION_NAME)
                .desc(LIST_TABLES_OPTION_DESCRIPTION)
                .build();
        commandOptions.addOption(currOption);
        currOption = Option.builder(EXPORT_OPTION_FLAG)
                .longOpt(EXPORT_OPTION_NAME)
                .desc(EXPORT_OPTION_DESCRIPTION)
                .argName(TABLE_NAMES_ARG_NAME)
                .optionalArg(true) // Allow no arguments
                .hasArgs() // Unlimited arguments
                .valueSeparator(',')
                .build();
        commandOptions.addOption(currOption);
        currOption = Option.builder(IMPORT_OPTION_FLAG)
                .longOpt(IMPORT_OPTION_NAME)
                .desc(IMPORT_OPTION_DESCRIPTION)
                .numberOfArgs(1)
                .argName(FILE_NAME_ARG_NAME)
                .build();
        commandOptions.addOption(currOption);
        commandOptions.setRequired(true);

        return commandOptions;
    }

    protected static String getLibraryName() {
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
        return DocumentDbDriver.DRIVER_VERSION;
    }

    private static Option buildVersionOption() {
        return Option.builder()
                .longOpt(VERSION_OPTION_NAME)
                .desc(VERSION_OPTION_DESCRIPTION)
                .build();
    }

    private static Options buildSshTunnelServiceOption() {
        return new Options().addOption(
                Option.builder()
                        .longOpt(SSH_TUNNEL_SERVICE_OPTION_NAME)
                        .desc(SSH_TUNNEL_SERVICE_OPTION_DESCRIPTION)
                        .numberOfArgs(1)
                        .argName(SSH_TUNNEL_SERVICE_ARG_NAME)
                        .build());
    }

    private static Option buildHelpOption() {
        return Option.builder(HELP_OPTION_FLAG)
                .longOpt(HELP_OPTION_NAME)
                .desc(HELP_OPTION_DESCRIPTION)
                .build();
    }

    private static @NonNull SimpleModule buildEnumLowerCaseSerializerModule() {
        final SimpleModule module = new SimpleModule();
        final JsonSerializer<Enum<?>> serializer = new StdSerializer<Enum<?>>(Enum.class, true) {
            @Override
            public void serialize(final Enum value, final JsonGenerator jGen,
                    final SerializerProvider provider) throws IOException {
                jGen.writeString(value.name().toLowerCase());
            }
        };
        module.addSerializer(serializer);
        return module;
    }

    @Getter
    private static class TableSchema {
        @JsonProperty(SQL_NAME_PROPERTY)
        private final String sqlName;
        @JsonProperty(COLLECTION_NAME_PROPERTY)
        private final String collectionName;
        @JsonProperty(COLUMNS_PROPERTY)
        private final List<DocumentDbSchemaColumn> columns;

        public TableSchema(final DocumentDbSchemaTable table) {
            this.sqlName = table.getSqlName();
            this.collectionName = table.getCollectionName();
            this.columns = ImmutableList.copyOf(table.getColumns());
        }

        @JsonCreator
        public TableSchema(
                @JsonProperty(SQL_NAME_PROPERTY)
                final String sqlName,
                @JsonProperty(COLLECTION_NAME_PROPERTY)
                final String collectionName,
                @JsonProperty(COLUMNS_PROPERTY)
                final List<DocumentDbSchemaColumn> columns) {
            this.sqlName = sqlName;
            this.collectionName = collectionName;
            this.columns = columns;
        }
    }

    private static class StringBuilderWriter extends Writer {
        private final StringBuilder stringBuilder;

        public StringBuilderWriter(final StringBuilder stringBuilder) {
            this.stringBuilder = stringBuilder;
        }

        @Override
        public void write(final char[] cBuf, final int off, final int len) {
            stringBuilder.append(cBuf, off, len);
        }

        @Override
        public void flush() {
            // noop
        }

        @Override
        public void close() {
            // noop
        }
    }
}
