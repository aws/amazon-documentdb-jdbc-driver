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

package software.amazon.documentdb.jdbc.common.utilities;

import org.slf4j.Logger;
import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Enum representing the possible error messages and lookup facilities for localization.
 */
public enum SqlError {
    AFTER_LAST,
    AUTHORIZATION_ERROR,
    BEFORE_FIRST,
    CANNOT_UNWRAP,
    CANNOT_RETRIEVE_COLUMN,
    CONN_CLOSED,
    CREATE_FOLDER_FAILED,
    DELETE_SCHEMA_FAILED,
    DELETE_TABLE_SCHEMA_FAILED,
    DELETE_TABLE_SCHEMA_INCONSISTENT,
    EQUIJOINS_ON_FK_ONLY,
    INCONSISTENT_SCHEMA,
    INVALID_COLUMN_LABEL,
    INVALID_CONNECTION_PROPERTIES,
    INVALID_FETCH_SIZE,
    INVALID_FORMAT,
    INVALID_LARGE_MAX_ROWS_SIZE,
    INVALID_MAX_FIELD_SIZE,
    INVALID_ROW_VALUE,
    INVALID_INDEX,
    INVALID_TIMEOUT,
    INVALID_STATE_SET_TABLE_FUNCTION,
    JOIN_MISSING_PRIMARY_KEYS,
    KNOWN_HOSTS_FILE_NOT_FOUND,
    MISSING_DATABASE,
    MISSING_HOSTNAME,
    MISSING_SCHEMA,
    MISSING_PASSWORD,
    MISSING_USER_PASSWORD,
    MISSING_LITERAL_VALUE,
    MISMATCH_SCHEMA_NAME,
    PARAMETERS_NOT_SUPPORTED,
    PASSWORD_PROMPT,
    POOLING_NOT_SUPPORTED,
    QUERY_CANCELED,
    QUERY_CANNOT_BE_CANCELED,
    QUERY_FAILED,
    QUERY_IN_PROGRESS,
    QUERY_NOT_STARTED_OR_COMPLETE,
    READ_ONLY,
    RESULT_FORWARD_ONLY,
    RESULT_SET_CLOSED,
    SECURITY_ERROR,
    SSH_PRIVATE_KEY_FILE_NOT_FOUND,
    SINGLE_EQUIJOIN_ONLY,
    SQL_PARSE_ERROR,
    STMT_CLOSED,
    TLS_CA_FILE_NOT_FOUND,
    TRANSACTIONS_NOT_SUPPORTED,
    UPSERT_SCHEMA_FAILED,
    UNSUPPORTED_CONVERSION,
    UNSUPPORTED_CROSS_REFERENCE,
    UNSUPPORTED_EXPORTED_KEYS,
    UNSUPPORTED_FETCH_DIRECTION,
    UNSUPPORTED_FUNCTIONS,
    UNSUPPORTED_FUNCTION_COLUMNS,
    UNSUPPORTED_GENERATED_KEYS,
    UNSUPPORTED_JOIN_TYPE,
    UNSUPPORTED_PREPARE_STATEMENT,
    UNSUPPORTED_PREPARE_CALL,
    UNSUPPORTED_PROCEDURE_COLUMNS,
    UNSUPPORTED_PROPERTY,
    UNSUPPORTED_PSEUDO_COLUMNS,
    UNSUPPORTED_REFRESH_ROW,
    UNSUPPORTED_RESULT_SET_TYPE,
    UNSUPPORTED_TABLE_PRIVILEGES,
    UNSUPPORTED_TYPE,
    UNSUPPORTED_SQL,
    UNSUPPORTED_SUPER_TABLES,
    UNSUPPORTED_SUPER_TYPES,
    UNSUPPORTED_USER_DEFINED_TYPES,
    UNSUPPORTED_VERSION_COLUMNS,
    ;

    private static final ResourceBundle RESOURCE = ResourceBundle.getBundle("jdbc");

    /**
     * Looks up the resource bundle string corresponding to the key, and formats it with the provided
     * arguments.
     *
     * @param key        Resource key for bundle provided to constructor.
     * @param formatArgs Any additional arguments to format the resource string with.
     * @return resource String, formatted with formatArgs.
     */
    public static String lookup(final SqlError key, final Object... formatArgs) {
        return String.format(RESOURCE.getString(key.name()), formatArgs);
    }

    /**
     * Get the error message and log the message.
     *
     * @param logger     The {@link Logger} contains log info.
     * @param key        Resource key for bundle provided to constructor.
     * @param formatArgs Any additional arguments to format the resource string with.
     * @return error massage
     */
    static String getErrorMessage(
            final Logger logger,
            final SqlError key,
            final Object... formatArgs) {
        final String error = lookup(key, formatArgs);
        logger.error(error);
        return error;
    }

    /**
     * Create {@link SQLException} of error and log the message with a {@link java.util.logging.Logger}.
     *
     * @param logger     The {@link java.util.logging.Logger} contains log info.
     * @param sqlState   A code identifying the SQL error condition.
     * @param key        Resource key for bundle provided to constructor.
     * @param formatArgs Any additional arguments to format the resource string with.
     * @return SQLException with error message.
     */
    public static SQLException createSQLException(
            final java.util.logging.Logger logger,
            final SqlState sqlState,
            final SqlError key,
            final Object... formatArgs) {
        final String error = lookup(key, formatArgs);
        logger.severe(error);
        return new SQLException(error, sqlState.getSqlState());
    }

    /**
     * Create SQLException of error and log the message with a {@link Logger}.
     *
     * @param logger     The {@link Logger} contains log info.
     * @param sqlState   A code identifying the SQL error condition.
     * @param key        Resource key for bundle provided to constructor.
     * @param formatArgs Any additional arguments to format the resource string with.
     * @return SQLException with error message.
     */
    public static SQLException createSQLException(
            final Logger logger,
            final SqlState sqlState,
            final SqlError key,
            final Object... formatArgs) {
        final String error = lookup(key, formatArgs);
        logger.error(error);
        return new SQLException(error, sqlState.getSqlState());
    }

    /**
     * Create SQLException of error and log the message with a {@link Logger}.
     *
     * @param logger     The {@link Logger} contains log info.
     * @param sqlState   A code identifying the SQL error condition.
     * @param exception  An {@link Exception} instance.
     * @param key        Resource key for bundle provided to constructor.
     * @param formatArgs Any additional arguments to format the resource string with.
     * @return SQLException with error message.
     */
    public static SQLException createSQLException(
            final Logger logger,
            final SqlState sqlState,
            final Exception exception,
            final SqlError key,
            final Object... formatArgs) {
        final String error = lookup(key, formatArgs);
        logger.error(error);
        return new SQLException(error, sqlState.getSqlState(), exception);
    }

    /**
     * Create {@link SQLFeatureNotSupportedException} of error and log the message with a {@link Logger}.
     *
     * @param logger     The {@link Logger} contains log info.
     * @param key        Resource key for bundle provided to constructor.
     * @param formatArgs Any additional arguments to format the resource string with.
     * @return SQLFeatureNotSupportedException with error message.
     */
    public static SQLFeatureNotSupportedException createSQLFeatureNotSupportedException(
            final Logger logger,
            final SqlError key,
            final Object... formatArgs) {
        final String error = lookup(key, formatArgs);
        logger.trace(error);
        return new SQLFeatureNotSupportedException(error);
    }

    /**
     * Create {@link SQLClientInfoException} of error and log the message with a {@link Logger}.
     *
     * @param logger     The {@link Logger} contains log info.
     * @param key        Resource key for bundle provided to constructor.
     * @param map        A Map containing the property values that could not be set.
     * @param formatArgs Any additional arguments to format the resource string with.
     * @return SQLClientInfoException with error message.
     */
    public static SQLClientInfoException createSQLClientInfoException(
            final Logger logger,
            final SqlError key,
            final Map<String, ClientInfoStatus> map,
            final Object... formatArgs) {
        final String error = lookup(key, formatArgs);
        logger.error(error);
        return new SQLClientInfoException(error, map);
    }
}

