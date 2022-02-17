# Connection String Syntax and Options
`jdbc:documentdb://[<user>[:<password>]@]<hostname>[:<port>]/<database-name>[?<option>=<value>[&<option>=<value>[...]]]`

### Scheme
`jdbc:documentdb:` Required: the scheme for this JDBC driver.

### Parameters
| Property | Description | Default |
|--------|-------------|---------------|
| `user` (optional) | The username of the authorized user. While the username is optional on the connection string, it is still required either via the connection string, or the properties. _Note: the username must be properly (%) encoded to avoid any confusion with URI special characters._ | `NONE`
| `password` (optional) | The password of the authorized user. While the password is optional on the connection string, it is still required either via the connection string, or the properties. _Note: the password must be properly (%) encoded to avoid any confusion with URI special characters._ | `NONE`
| `hostname` (required) | The hostname or IP address of the DocumentDB server or cluster. | `NONE`
| `port` (optional) | The port number the DocumentDB server or cluster is listening on. | `27017`
| `database` (required) | The name of the database the JDBC driver will connect to. | `NONE`
| `option` (optional) | One of the connection string options listed below. | `NONE`
| `value` (optional) | The associated value for the option. | `NONE`

### Options
| Option | Description | Default |
|--------|-------------|---------------|
| `appName` | (string) Sets the logical name of the application. | `Amazon DocumentDB JDBC Driver {version}`
| `loginTimeoutSec` | (int) How long a connection can take to be opened before timing out (in seconds). Alias for connectTimeoutMS but using seconds. | `NONE`
| `readPreference` | (enum/string) The read preference for this connection. Allowed values: `primary`, `primaryPreferred`, `secondary`, `secondaryPreferred` or `nearest`. | `primary`
| `replicaSet` | (string) Name of replica set to connect to. For now, passing a name other than `rs0` will log a warning. | `NONE`
| `retryReads` | (true/false) If true, the driver will retry supported read operations if they fail due to a network error. | `true`
| `tls` | (true/false) If true, use TLS encryption when communicating with the DocumentDB server. | `true`
| `tlsAllowInvalidHostnames` | (true/false) If true, invalid host names for the TLS certificate are allowed. This is useful when using an internal SSH tunnel to a DocumentDB server. | `false`
| `tlsCAFile` | (string) The path to the trusted Certificate Authority (CA) `.pem` file. If the path starts with the tilde character (`~`), it will be replaced with the user's home directory. Ensure to use only forward slash characters (`/`) in the path or URL encode the path. Providing the trusted Certificate Authority (CA) `.pem` file is optional as the current Amazon RDS root CA is used by default when the `tls` option is set to `true`. This embedded certificate is set to expire on 2024-08-22. For example, to provide a new trusted Certificate Authority (CA) `.pem` file that is located in the current user's `Downloads` subdirectory of their home directory, use the following: `tlsCAFile=~/Downloads/rds-ca-2019-root.pem`. | `NONE`
| `sshUser` | (string) The username for the internal SSH tunnel. If provided, options `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | `NONE`
| `sshHost` | (string) The host name for the internal SSH tunnel. Optionally the SSH tunnel port number can be provided using the syntax `<ssh-host>:<port>`. The default port is `22`. If provided, options `sshUser` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | `NONE`
| `sshPrivateKeyFile` | (string) The path to the private key file for the internal SSH tunnel. If the path starts with the tilde character (`~`), it will be replaced with the user's home directory. If the path is relative, the absolute path will try to be resolved by searching in the user's home directory (`~`), the `.documentdb` folder under the user's home directory or in the same directory as the driver JAR file. If the file cannot be found, a connection error will occur. If provided, options `sshUser` and `sshHost` must also be provided, otherwise this option is ignored. | `NONE`
| `sshPrivateKeyPassphrase` | (string) If the SSH tunnel private key file, `sshPrivateKeyFile`, is passphrase protected, provide the passphrase using this option. If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | `NONE`
| `sshStrictHostKeyChecking` | (true/false) If true, the 'known_hosts' file is checked to ensure the target host is trusted when creating the internal SSH tunnel. If false, the target host is not checked. Disabling this option is less secure as it can lead to a ["man-in-the-middle" attack](https://en.wikipedia.org/wiki/Man-in-the-middle_attack). If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | `true`
| `sshKnownHostsFile` | (string) The path to the 'known_hosts' file used for checking the target host for the SSH tunnel when option `sshStrictHostKeyChecking` is `true`. The `known_hosts` file can be populated using the `ssh-keyscan` [tool](maintain_known_hosts.md). If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this option is ignored. | `~/.ssh/known_hosts`
| `scanMethod` | (enum/string) The scanning (sampling) method to use when discovering collection metadata for determining table schema. Possible values include the following: 1) `random` - the sample documents are returned in _random_ order, 2) `idForward` - the sample documents are returned in order of id, 3) `idReverse` - the sample documents are returned in reverse order of id or 4) `all` - sample all the documents in the collection. | `random`
| `scanLimit` | (int) The number of documents to sample. The value must be a positive integer. If `scanMethod` is set to `all`, this option is ignored. | `1000`
| `schemaName` | (string) The name of the SQL mapping schema for the database. | `_default`.  
| `defaultFetchSize` | (int) The default fetch size (in records) when retrieving results from Amazon DocumentDB. It is the number of records to retrieve in a single batch. The maximum number of records retrieved in a single batch may also be limited by the overall memory size of the result. The value can be changed by calling the `Statement.setFetchSize` JDBC method. | `2000`
| `refreshSchema` | (true/false) If true, generates (refreshes) the SQL schema with each connection. It creates a new version, leaving any existing versions in place. _Caution: use only when necessary to update schema as it can adversely affect performance._  | `false`
| `defaultAuthDb` | (string) The name of the authentication database to use when authenticating with the passed `user` and `password`. This is where the authorized user is stored and can be different from what databases the user may have access to. On Amazon DocumentDB, all users are attributed to the `admin` database. | `admin`

## Examples

### Connecting to an Amazon DocumentDB Cluster

```
jdbc:documentdb://localhost/customer?tlsAllowInvalidHostnames=true
```

#### Notes:

1. An external [SSH tunnel](setup.md#using-an-ssh-tunnel-to-connect-to-amazon-documentdb) is being used where the local 
port is `27017` (`27017` is default).
2. The Amazon DocumentDB database name is `customer`.
3. The Amazon DocumentDB is TLS-enabled (`tls=true` is default)
4. User and password values are passed to the JDBC driver using **Properties**.

### Connecting to an Amazon DocumentDB Cluster on Non-Default Port

```
jdbc:documentdb://localhost:27117/customer?tlsAllowInvalidHostnames=true
```

#### Notes:

1. An external [SSH tunnel](setup.md#using-an-ssh-tunnel-to-connect-to-amazon-documentdb) is being used where the local 
port is `27117`.
1. The Amazon DocumentDB database name is `customer`.
1. The Amazon DocumentDB is TLS-enabled (`tls=true` is default).
1. User and password values are passed to the JDBC driver using **Properties**.

### Connecting to an Amazon DocumentDB Cluster using an Internal SSH tunnel

```
jdbc:documentdb://docdb-production.docdb.amazonaws.com/customer?tlsAllowInvalidHostnames=true&sshUser=ec2-user&sshHost=ec2-254-254-254-254.compute.amazonaws.com&sshPrivateKeyFile=~/.ssh/ec2-privkey.pem
```

#### Notes:

1. DocumentDB cluster host is `docdb-production.docdb.amazonaws.com` (using default port `27017`).
2. The Amazon DocumentDB database name is `customer`.
3. The Amazon DocumentDB is TLS-enabled (`tls=true` is default).
4. An internal SSH tunnel will be created using the user `ec2-user`,
   host `ec2-254-254-254-254.compute.amazonaws.com`, and private key file `~/.ssh/ec2-privkey.pem`.
6. User and password values are passed to the JDBC driver using **Properties**.

### Change the Scanning Method when Connecting to an Amazon DocumentDB Cluster

```
jdbc:documentdb://localhost/customer?tlsAllowInvalidHostnames=true&scanMethod=idForward&scanLimit=5000
```

#### Notes:

1. An external [SSH tunnel](setup.md#using-an-ssh-tunnel-to-connect-to-amazon-documentdb) is being used where the 
local port is `27017` (`27017` is default).
2. The Amazon DocumentDB database name is `customer`.
3. The Amazon DocumentDB is TLS-enabled (`tls=true` is default).
4. User and password values are passed to the JDBC driver using **Properties**.
5. The scan method `idForward` will order the result using the `_id` column in the collection.
6. The scan limit `5000` will limit the number of scanned documents to 5000.
