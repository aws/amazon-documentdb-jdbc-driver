(function propertiesbuilder(attr) {
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

    // Get optional parameters.
    var params = {};
    params[TLS_ENABLED_KEY] = attr[connectionHelper.attributeSSLMode] === "require" ? "true" : "false";
    params[TLS_ALLOW_INVALID_HOSTNAMES_KEY] = attr["v-allow-invalid-hostnames"];
    params[READ_PREFERENCE_KEY] = attr["v-read-preference"];
    params[SCAN_METHOD_KEY] = attr["v-scan-method"];
    params[RETRY_READS_KEY] = attr["v-retry-reads"];
    params[LOGIN_TIMEOUT_KEY] = attr["v-login-timeout"];

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
