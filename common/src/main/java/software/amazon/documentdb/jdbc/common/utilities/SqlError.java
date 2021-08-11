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
    AAD_ACCESS_TOKEN_ERROR,
    ASYNC_RETRIEVAL_ERROR,
    AAD_ACCESS_TOKEN_REQUEST_FAILED,
    AFTER_LAST,
    AUTHORIZATION_ERROR,
    BEFORE_FIRST,
    CANNOT_UNWRAP,
    CANNOT_CONVERT_STRING_TO_RESULT_SET,
    CANNOT_RETRIEVE_COLUMN,
    CANNOT_SLICE_A_STRING,
    CONN_CLOSED,
    CONN_FAILED,
    CREATE_FOLDER_FAILED,
    DELETE_SCHEMA_FAILED,
    DELETE_TABLE_SCHEMA_FAILED,
    EQUIJOINS_ON_FK_ONLY,
    FAILED_TO_BUFFER_RESULT_SET,
    FAILED_TO_NOTIFY_CONSUMER_THREAD,
    FAILED_TO_PROPAGATE_ERROR,
    FAILED_TO_SHUTDOWN_RETRIEVAL_EXECUTOR_SERVICE,
    INCONSISTENT_SCHEMA,
    INCORRECT_SOURCE_TYPE_AT_CELL,
    INVALID_AAD_ACCESS_TOKEN_RESPONSE,
    INVALID_COLUMN_LABEL,
    INVALID_CONNECTION_PROPERTIES,
    INVALID_CREDENTIALS_FILE_PATH,
    INVALID_DATA_AT_ARRAY,
    INVALID_DATA_AT_ROW,
    INVALID_ENDPOINT,
    INVALID_FETCH_SIZE,
    INVALID_FORMAT,
    INVALID_LARGE_MAX_ROWS_SIZE,
    INVALID_MAX_CONNECTIONS,
    INVALID_MAX_FIELD_SIZE,
    INVALID_MAX_RETRY_COUNT,
    INVALID_NUMERIC_CONNECTION_VALUE,
    INVALID_ROW_VALUE,
    INVALID_INDEX,
    INVALID_TIMEOUT,
    INVALID_TYPE,
    INVALID_QUERY,
    INVALID_SAML_RESPONSE,
    INVALID_SESSION_TOKEN_RESPONSE,
    INVALID_STATE_SET_TABLE_FUNCTION,
    KNOWN_HOSTS_FILE_NOT_FOUND,
    MISSING_DATABASE,
    MISSING_HOSTNAME,
    MISSING_REQUIRED_IDP_PARAMETER,
    MISSING_SCHEMA,
    MISSING_SERVICE_REGION,
    MISSING_PASSWORD,
    MISSING_USER_PASSWORD,
    MISMATCH_SCHEMA_NAME,
    OKTA_SAML_ASSERTION_ERROR,
    OKTA_SAML_ASSERTION_REQUEST_FAILED,
    OKTA_SESSION_TOKEN_REQUEST_FAILED,
    OKTA_SESSION_TOKEN_ERROR,
    PARAMETERS_NOT_SUPPORTED,
    PASSWORD_PROMPT,
    POOLING_NOT_SUPPORTED,
    QUERY_CANCELED,
    QUERY_CANNOT_BE_CANCELED,
    QUERY_FAILED,
    QUERY_IN_PROGRESS,
    QUERY_NOT_STARTED_OR_COMPLETE,
    QUERY_TIMED_OUT,
    READ_ONLY,
    RESULT_FORWARD_ONLY,
    RESULT_SET_CLOSED,
    SECURITY_ERROR,
    SINGLE_EQUIJOIN_ONLY,
    SQL_PARSE_ERROR,
    STMT_CLOSED,
    STMT_CLOSED_DURING_EXECUTE,
    TLS_CA_FILE_NOT_FOUND,
    TRANSACTIONS_NOT_SUPPORTED,
    UPSERT_SCHEMA_FAILED,
    UNSUPPORTED_AWS_CREDENTIALS_PROVIDER,
    UNSUPPORTED_BINARY_STREAM,
    UNSUPPORTED_CLASS,
    UNSUPPORTED_COLUMN_PRIVILEGES,
    UNSUPPORTED_CONVERSION,
    UNSUPPORTED_CROSS_REFERENCE,
    UNSUPPORTED_EXPORTED_KEYS,
    UNSUPPORTED_FETCH_DIRECTION,
    UNSUPPORTED_FUNCTIONS,
    UNSUPPORTED_FUNCTION_COLUMNS,
    UNSUPPORTED_GENERATED_KEYS,
    UNSUPPORTED_JOIN_TYPE,
    UNSUPPORTED_MULTIPLE_HOSTS,
    UNSUPPORTED_PREPARE_STATEMENT,
    UNSUPPORTED_PREPARE_CALL,
    UNSUPPORTED_PROCEDURE_COLUMNS,
    UNSUPPORTED_PROPERTY,
    UNSUPPORTED_PSEUDO_COLUMNS,
    UNSUPPORTED_REFRESH_ROW,
    UNSUPPORTED_RESULT_SET_TYPE,
    UNSUPPORTED_TABLE_PRIVILEGES,
    UNSUPPORTED_TYPE,
    UNSUPPORTED_SAML_CREDENTIALS_PROVIDER,
    UNSUPPORTED_SCHEMA,
    UNSUPPORTED_SQL,
    UNSUPPORTED_SUPER_TABLES,
    UNSUPPORTED_SUPER_TYPES,
    UNSUPPORTED_USER_DEFINED_TYPES,
    UNSUPPORTED_VERSION_COLUMNS,
    VALUE_OUT_OF_RANGE;

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

