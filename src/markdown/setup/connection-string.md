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
  are allowed. This is useful when using an SSH tunnel to a DocumentDB server. Defaults to false.
- `tlsCAFile` (string) : The path to the trusted Certificate Authority (CA) `.pem` file. If the
  path starts with the tilde character (`~`), it will be replaced with the user's home directory.
  Ensure to use only forward slash characters (`/`) in the path or URL encode the path. Providing
  the trusted Certificate Authority (CA) `.pem` file is optional as the current Amazon RDS root CA
  is used by default when the `tls` option is set to `true`. This embedded certificate is set to
  expire on 2024-08-22. For example, to provide a new trusted Certificate Authority (CA) `.pem`
  file that is located in the current user's `Downloads` subdirectory of their home directory, 
  use the following: `tlsCAFile=~/Downloads/rds-ca-2019-root.pem`.
- `scanMethod` (enum/string) : The scanning (sampling) method to use when discovering collection
  metadata for determining table schema. Possible values include the following:
    - `random` - (default) The sample documents are returned in _random_ order.
    - `idForward` - The sample documents are returned in order of id.
    - `idReverse` - The sample documents are returned in reverse order of id.
    - `all` - Sample all the documents in the collection.
- `scanLimit` (int) The number of documents to sample. The value must be a positive integer.
  The default value is `1000`. If `scanMethod` is set to `all`, this option is ignored.
- `schemaName` (string) The name of the database schema. Defaults to `_default`.  

## Examples

### Connecting to an Amazon DocumentDB Cluster

```
jdbc:documentdb://localhost/customer?tlsAllowInvalidHostnames=true
```

#### Notes:

1. An [SSH tunnel](ssh-tunnel.md) is being used where the local port is `27017` (`27017` is default).
1. The Amazon DocumentDB database name is `customer`.
1. The Amazon DocumentDB is TLS-enabled (`tls=true` is default)
1. User and password values are passed to the JDBC driver using **Properties**.

### Connecting to an Amazon DocumentDB Cluster on Non-Default Port

```
jdbc:documentdb://localhost:27117/customer?tlsAllowInvalidHostnames=true
```

#### Notes:

1. An SSH tunnel is being used where the local port is `27117`.
1. The Amazon DocumentDB database name is `customer`.
1. The Amazon DocumentDB is TLS-enabled (`tls=true` is default).
1. User and password values are passed to the JDBC driver using **Properties**.

### Change the Scanning Method when Connecting to an Amazon DocumentDB Cluster

```
jdbc:documentdb://localhost/customer?tlsAllowInvalidHostnames=true&scanMethod=idForward&scanLimit=5000
```

#### Notes:

1. An SSH tunnel is being used where the local port is `27017` (`27017` is default).
1. The Amazon DocumentDB database name is `customer`.
1. The Amazon DocumentDB is TLS-enabled (`tls=true` is default).
1. User and password values are passed to the JDBC driver using **Properties**.
1. The scan method `idForward` will order the result using the `_id` column in the collection.
1. The scan limit `5000` will limit the number of scanned documents to 5000.
