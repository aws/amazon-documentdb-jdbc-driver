(function propertiesbuilder(attr) {
    logging.Log("attr=" + JSON.stringify(attr));

    // Set properties keys.
    const USER_KEY = "user";
    const PASSWORD_KEY = "password";
    const APP_NAME_KEY = "appName";
    const TLS_ENABLED_KEY = "tls";
    const TLS_ALLOW_INVALID_HOSTNAMES_KEY = "tlsAllowInvalidHostnames";
    const READ_PREFERENCE_KEY = "readPreference";
    const REPLICA_SET_KEY = "replicaSet";
    const SCAN_METHOD_KEY = "scanMethod";
    const SCAN_LIMIT_KEY = "scanLimit";
    const RETRY_READS_KEY = "retryReads";
    const LOGIN_TIMEOUT_KEY= "loginTimeoutSec";
    const TLS_CA_FILE_KEY="tlsCAFile"
    const SCHEMA_NAME="schemaName"
    const SSH_USER="sshUser"
    const SSH_HOST="sshHost"
    const SSH_PRIV_KEY_FILE="sshPrivateKeyFile"
    const SSH_PRIVATE_KEY_PASSPHRASE="sshPrivateKeyPassphrase"
    const SSH_STRICT_HOST_KEY_CHECKING="sshStrictHostKeyChecking"
    const SSH_KNOWN_HOSTS_FILE="sshKnownHostsFile"
    const REFRESH_SCHEMA="refreshSchema"

    // Get optional parameters.
    var params = {};
    params[TLS_ENABLED_KEY] = attr[connectionHelper.attributeSSLMode] === "require" ? "true" : "false";
    params[TLS_ALLOW_INVALID_HOSTNAMES_KEY] = attr["v-allow-invalid-hostnames"];
    params[READ_PREFERENCE_KEY] = attr["v-read-preference"];
    params[SCAN_METHOD_KEY] = attr["v-scan-method"];
    params[RETRY_READS_KEY] = attr["v-retry-reads"];
    params[LOGIN_TIMEOUT_KEY] = attr["v-login-timeout"];
    params[TLS_CA_FILE_KEY] = attr["v-tls-ca-file"];
    params[SSH_USER] = attr["v-ssh-user"];
    params[SSH_HOST] = attr["v-ssh-host"];
    params[SSH_PRIV_KEY_FILE] = attr["v-ssh-priv-key-filename"];
    params[SSH_PRIVATE_KEY_PASSPHRASE] = attr["v-ssh-priv-key-passphrase"];
    params[SSH_STRICT_HOST_KEY_CHECKING] = attr["v-ssh-strict-host-key-check"];
    params[SSH_KNOWN_HOSTS_FILE] = attr["v-ssh-known-hosts-file"];
    params[REFRESH_SCHEMA] = attr["v-refresh-schema"];

    // Add schema name if set.
    if (attr["v-schema-name"]) {
        params[SCHEMA_NAME] = attr["v-schema-name"];
    }

    // Use default (and only) replica set.
    if (attr["v-replica-set"] === "true") {
        params[REPLICA_SET_KEY] = "rs0";
    }

    // Add scan limit if set.
    if (attr["v-scan-limit"]) {
        params[SCAN_LIMIT_KEY] = attr["v-scan-limit"];
    }

    // App name is an "optional" parameter but use a default application name instead.
    params[APP_NAME_KEY] = "Tableau";

    // Get secure parameters.
    params[USER_KEY] = attr[connectionHelper.attributeUsername];
    params[PASSWORD_KEY] = attr[connectionHelper.attributePassword];

    var formattedParams = [];
    for (var key in params) {
        var pair = connectionHelper.formatKeyValuePair(key, params[key]);
        formattedParams.push(pair);
    }

    return formattedParams;
})
