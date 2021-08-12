# Connection String Syntax and Options

`jdbc:documentdb://[<user>[:<password>]@]<hostname>[:<port>]/<database-name>[?<option>=<value>[&<option>=<value>[...]]]`

### Scheme

`jdbc:documentdb:` Required: the scheme for this JDBC driver.

### Parameters

- `<user>` Optional: the username of the authorized user. While the username is optional on the
  connection string, it is still required either via the connection string, or the properties.
  _Note: the username must be properly (%) encoded to avoid any confusion with URI special
  characters._
- `<password>` Optional: the password of the authorized user. While the password is optional on the
  connection string, it is still required either via the connection string, or the properties.
  _Note: the password must be properly (%) encoded to avoid any confusion with URI special
  characters._
- `<hostname>` Required: the hostname or IP address of the DocumentDB server or cluster.
- `<port>` Optional: the port number the DocumentDB server or cluster is listening on.
- `<database>` Required: the name of the database the JDBC driver will connect to.
- `<option>` Optional: one of the connection string options listed below.
- `<value>` Optional: the associated value for the option.

### Options

- `appName` (string) : Sets the logical name of the application.
- `loginTimeoutSec` (int) : How long a connection can take to be opened before timing out (in seconds).
  Alias for connectTimeoutMS but using seconds.
- `readPreference` (enum/string) : The read preference for this connection. Allowed values:
    - `primary` (default)
    - `primaryPreferred`
    - `secondary`
    - `secondaryPreferred`
    - `nearest`.
- `replicaSet` (string) : Name of replica set to connect to. For now, passing a name other than
  `rs0` will log a warning.
- `retryReads` (true|false) : If true, the driver will retry supported read operations if they
  fail due to a network error. Defaults to `true`.
- `tls` (true|false) : If true, use TLS encryption when communicating with the DocumentDB server.
  Defaults to `true`.
- `tlsAllowInvalidHostnames` (true|false) : If true, invalid host names for the TLS certificate
  are allowed. This is useful when using an internal SSH tunnel to a DocumentDB server. Defaults to false.
- `tlsCAFile` (string) : The path to the trusted Certificate Authority (CA) `.pem` file. If the
  path starts with the tilde character (`~`), it will be replaced with the user's home directory.
  Ensure to use only forward slash characters (`/`) in the path or URL encode the path. Providing
  the trusted Certificate Authority (CA) `.pem` file is optional as the current Amazon RDS root CA
  is used by default when the `tls` option is set to `true`. This embedded certificate is set to
  expire on 2024-08-22. For example, to provide a new trusted Certificate Authority (CA) `.pem`
  file that is located in the current user's `Downloads` subdirectory of their home directory,
  use the following: `tlsCAFile=~/Downloads/rds-ca-2019-root.pem`.
- `sshUser` (string) : The username for the internal SSH tunnel. 
  If provided, options `sshHost` and `sshPrivateKeyFile` must also be provided, otherwise this
  option is ignored.
- `sshHost` (string) : The host name for the internal SSH tunnel. Optionally the SSH tunnel port number can be
  provided using the syntax `<ssh-host>:<port>`. The default port is `22`.
  If provided, options `sshUser` and `sshPrivateKeyFile` must also be provided, otherwise this
  option is ignored. 
- `sshPrivateKeyFile` (string) : The path to the private key file for the internal SSH tunnel. If the
  path starts with the tilde character (`~`), it will be replaced with the user's home directory.
  If provided, options `sshUser` and `sshHost` must also be provided, otherwise this option 
  is ignored.
- `sshPrivateKeyPassphrase` (string) : If the SSH tunnel private key file, `sshPrivateKeyFile`, is
  passphrase protected, provide the passphrase using this option.
  If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided,
  otherwise this option is ignored.
- `sshStrictHostKeyChecking` (true|false) : If true, the 'known_hosts' file is checked to ensure 
  the target host is trusted when creating the internal SSH tunnel. If false, the target host is not checked.
  Default is `true`. Disabling this option is less secure as it can lead to a 
  ["man-in-the-middle" attack](https://en.wikipedia.org/wiki/Man-in-the-middle_attack).
  If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided,
  otherwise this option is ignored.
- `sshKnownHostsFile` (string) : The path to the 'known_hosts' file used for checking the target 
  host for the SSH tunnel when option `sshStrictHostKeyChecking` is '`true`.
  Default is `~/.ssh/known_hosts`. The `known_hosts` file
  can be populated using the `ssh-keygen` [tool](https://www.ssh.com/academy/ssh/keygen).
  If provided, options `sshUser`, `sshHost` and `sshPrivateKeyFile` must also be provided,
  otherwise this option is ignored.
- `scanMethod` (enum/string) : The scanning (sampling) method to use when discovering collection
  metadata for determining table schema. Possible values include the following:
    - `random` - (default) The sample documents are returned in _random_ order.
    - `idForward` - The sample documents are returned in order of id.
    - `idReverse` - The sample documents are returned in reverse order of id.
    - `all` - Sample all the documents in the collection.
- `scanLimit` (int) The number of documents to sample. The value must be a positive integer.
  The default value is `1000`. If `scanMethod` is set to `all`, this option is ignored.
- `schemaName` (string) The name of the SQL mapping schema for the database. Defaults to `_default`.  

## Examples

### Connecting to an Amazon DocumentDB Cluster

```
jdbc:documentdb://localhost/customer?tlsAllowInvalidHostnames=true
```

#### Notes:

1. An external [SSH tunnel](ssh-tunnel.md) is being used where the local port is `27017` (`27017` is default).
1. The Amazon DocumentDB database name is `customer`.
1. The Amazon DocumentDB is TLS-enabled (`tls=true` is default)
1. User and password values are passed to the JDBC driver using **Properties**.

### Connecting to an Amazon DocumentDB Cluster on Non-Default Port

```
jdbc:documentdb://localhost:27117/customer?tlsAllowInvalidHostnames=true
```

#### Notes:

1. An external [SSH tunnel](ssh-tunnel.md) is being used where the local port is `27117`.
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

1. An external [SSH tunnel](ssh-tunnel.md) is being used where the local port is `27017` (`27017` is default).
1. The Amazon DocumentDB database name is `customer`.
1. The Amazon DocumentDB is TLS-enabled (`tls=true` is default).
1. User and password values are passed to the JDBC driver using **Properties**.
1. The scan method `idForward` will order the result using the `_id` column in the collection.
1. The scan limit `5000` will limit the number of scanned documents to 5000.
