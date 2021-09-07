# Troubleshooting Guide

- [Common Issues](#common-issues)
   * [Connection Issues](#connection-issues)
   * [Schema Issues](#schema-issues)
   * [Query Issues](#query-issues)
- [Logs](#logs)

## Common Issues
### Connection Issues

#### Invalid host or port number

##### What to look for:

-  `java.net.ConnectException: Connection refused: connect`

###### Tableau Connection Error

```text
An error occurred while communicating with Amazon DocumentDB by AWS

Bad Connection: Tableau could not connect to the data source.

Error Code: FAB9A2C5

Timed out after 30000 ms while waiting to connect. 
Client view of cluster state is {type=UNKNOWN, servers=[{address=localhost:27017, type=UNKNOWN, state=CONNECTING, 
exception={com.mongodb.MongoSocketOpenException: Exception opening socket},
 caused by {java.net.ConnectException: Connection refused: connect}}]
 
Unable to connect to the Amazon DocumentDB by AWS server "localhost". 
Check that the server is running and that you have access privileges to the requested database.

Connector Class: documentdbjdbc, Version: ...
```

###### DbVisualizer Connection Error

```text
An error occurred while establishing the connection:

Long Message:

Timed out after 30000 ms while waiting to connect. 
Client view of cluster state is {type=UNKNOWN, servers=[{address=localhost:27017, type=UNKNOWN, state=CONNECTING, 
exception={com.mongodb.MongoSocketOpenException: Exception opening socket},
 caused by {java.net.ConnectException: Connection refused: connect}}]
```

##### What to do:
This problem suggests that you are unable to reach the DocumentDB cluster. Most commonly, this is 
because you are outside the cluster's VPC. To connect to an Amazon DocumentDB cluster from outside 
an Amazon VPC, you can use an SSH tunnel. The driver allows users to set up an SSH tunnel manually 
or pass additional parameters in the connecting string so that the driver can create one automatically. 
See [Using an SSH Tunnel to Connect to Amazon DocumentDB](../setup/setup.md#using-an-ssh-tunnel-to-connect-to-amazon-documentdb) 
for more information on both methods. 

If this is still an issue after having set up an SSH tunnel, double-check the values of *Port* or 
*Hostname*.

If you have manually set up an SSH tunnel:
1. Ensure the *Port* number in your connection string matches the local port number configured when using an SSH tunnel. 
   In this example, the local port is **27019** not 27017:
   <pre>
   <code> ssh -i "ec2Access.pem" -L <b>27019</b>:sample-cluster.node.us-east-1.docdb.amazonaws.com:27017 ec2-user@ec2.compute-1.amazonaws.com -N </code>
   </pre>
1. Ensure the *Hostname* in your connection string is set to `localhost`.

If you are using the automatically set up SSH tunnel:  
1. Ensure the *Port* number in your connection matches the cluster's port number. Typically, this is `27017`. 
1. Ensure the *Hostname* in your connection string is set to the cluster hostname and **not** `localhost`.

If the issue is unrelated to SSH tunneling, 
double check other parameters in your [connection string](../setup/connection-string.md)
and make sure the cluster is running and available to connect to.

#### Internally Embedded Certificate Authority Issue       

##### What to look for:

-  `unable to find valid certification path to requested target`
-  `Server's certificate with common name ... is not trusted`

###### Tableau Certificate Error

```text
An error occurred while communicating with Amazon DocumentDB by AWS

Bad Connection: Tableau could not connect to the data source.

Error Code: FAB9A2C5

Timed out after 30000 ms while waiting to connect. 
Client view of cluster state is {type=UNKNOWN, servers=[{address=localhost:27019, type=UNKNOWN, state=CONNECTING, 
exception={com.mongodb.MongoSocketWriteException: Exception sending message},
 caused by {javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target},
 caused by {sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target}, 
 caused by {sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target}}]
 
Unable to connect to the Amazon DocumentDB by AWS server "localhost". 
Check that the server is running and that you have access privileges to the requested database.

Connector Class: documentdbjdbc, Version: ...
```

###### DbVisualizer Certificate Error

```text
Server's certificate with common name ... is not trusted.

The reason a server is not trusted is most likely due to missing certificate authorities in the truststore. 
The online security resources may give a pointer how to fix this.
```

#### What to do:

1. Download the latest root certificate authority file. 
   See [Specifying the Amazon RDS Certificate Authority Certificate File](../setup/setup.md#specifying-the-amazon-rds-certificate-authority-certificate-file). 
1. Copy the file to your home directory.
1. Provide the root certificate file name in the connection. 
   - Tableau: *TLS Certificate Authority File (Optional)* : `~/rds-ca-2019-root.pem`
   - DbVisualizer: `jdbc:documentdb://localhost:27017/test?tls=true&tlsAllowInvalidHostnames=true&tlsCAFile=~/rds-ca-2019-root.pem`
    
### Invalid hostname 
#### What to look for: 
-  `javax.net.ssl.SSLHandshakeException: No subject alternative DNS name matching localhost found.`

#### What to do: 
1. Pass the `tlsAllowInvalidHostnames` parameter in your connection string as `true`. 

### Invalid username or password 

#### What to look for: 
- `Authorization failed for user '<username>' on database 'admin' with mechanism 'SCRAM-SHA-1'.`
- `Exception authenticating MongoCredential{mechanism=SCRAM-SHA-1, userName='<username>', source='admin', password=<hidden>, mechanismProperties=<hidden>}'.`

#### What to do: 
1. Double check that your username and password are correct. 
2. Make sure that the user you are logging in with has the right access privileges to the target database.

## Schema Issues

### Schema Out of Date

#### What to look for: 
- Cannot find new collection(s) as tables in SQL schema.
- Cannot find new document field(s) as columns in SQL table schema.

#### What to do:

1. To update the SQL schema for your database, run the command line interface to 
[generate a new schema](../schema/manage-schema-cli.md).
   For example:
    ```text
    java -jar document-db-1.0.0-all.jar --generate-new \
    --server localhost:27019 --database test -u ajones --tls --tls-allow-invalid-hostnames
    ``` 
    
If you have set up the `mongo` shell or have set up a tool like 
[MongoDB Compass](https://www.mongodb.com/products/compass), you can also 
manually delete the old schemas. Exercise caution with this method.

1. Connect to the cluster using your tool of choice.
1. Navigate to your target database. 
1. Drop the collections `_sqlSchemas` and `_sqlTableSchemas`. 
   On the `mongo` shell, this can be done as follows: 
   ```
   use <database> 
   db.getCollection("_sqlSchemas").drop() 
   db.getCollection("_sqlTableSchemas").drop()
   ```
   Note that the collections cannot be dropped with the typical command `db._sqlSchemas.drop()`
   because their names start with underscores `_`.
### Query Issues

##### What to look for: 

- `Unsupported SQL syntax  ...`
- `Unable to parse SQL ...`

##### What to do: 
1. Check that your query follows the [expected format](../sql/sql-limitations.md#basic-query-format).
1. Check that the [identifiers](../sql/sql-limitations.md#identifiers) 
   matching a SQL keyword are either quoted using `"` or are using fully-qualified names (ex: `table.column`). 
   Using double quotes and fully-qualified names for all queries in general is recommended. 
   Note that using single quotes in place of double quotes is not allowed and that all identifiers are case-sensitive.
1. Check that you are not terminating your statement with a semicolon `;`. This is not allowed.  
1. If using any literals, check that they are [formatted correctly](../sql/sql-limitations.md#data-types). 
   Note that literals use single quotes instead of double quotes.
1. If using any [operators or functions](../sql/sql-limitations.md#operators-and-functions), 
   check that they are supported and are being called correctly.
1. For simple validation errors, the error message may contain a `Reason:` segment. Use 
   this to troubleshoot when possible. For example, this error can be resolved 
   by correcting `TESTCOLLECTION` to `testCollection`:
   <pre>
   <code> Unable to parse SQL 'SELECT * FROM database.TESTCOLLECTION'."
    <b>Reason:</b> 'From line 1, column 15 to line 1, column 37:"
    Object 'TESTCOLLECTION' not found within 'database'; did you mean 'testCollection'?'" </code>
   </pre>

## Logs

When troubleshooting, it can be helpful to view the logs so that you might be able 
to resolve the issue on your own or at least have more context to provide when seeking support.  
The driver's logs will be written to `/tmp/logs/DocumentDB_JDBC.log`.

Many BI tools also provide an interface for users to easily view logs. Tableau, for example, 
has [Tableau Log Viewer](https://github.com/tableau/tableau-log-viewer). 
Tableau Desktop stores the logs in the `Logs` subfolder under `My Tableau Repository` which is typically 
under `Documents` although this may vary depending on how Tableau was installed. 
The JDBC driver's logs will be in the `jprotocolserver.log` file. 

Refer to the documentation of your tool of choice for similar instructions.

### Setting Logging Level and Location
There are the following levels of logging:

| Property Value | Description |
|--------|-------------|
| `FATAL` | Shows messages at a FATAL level only.|
| `ERROR` | Shows messages classified as ERROR and FATAL.|
| `WARNING` | Shows messages classified as WARNING, ERROR and FATAL.|
| `INFO` | Shows messages classified as INFO, WARNING, ERROR and FATAL.|
| `DEBUG` | Shows messages classified as DEBUG, INFO, WARNING, ERROR and FATAL.|
| `TRACE` | Shows messages classified as TRACE, DEBUG, INFO, WARNING, ERROR and FATAL.|
| `ALL` | Shows messages classified as TRACE, DEBUG, INFO, WARNING, ERROR and FATAL.|
| `OFF` | No log messages displayed.|

| Property Name | Description | Default |
|--------|-------------|---------------|
| `documentdb.jdbc.log.level` | The log level for all sources/appenders. | `INFO` |
| `documentdb.jdbc.log.file.path` | The location for file logging. | `~/.documentdb/logs/documentdb-jdbc.log` |
| `documentdb.jdbc.log.file.threshold` | The threshold for file logging. | `ALL` |
| `documentdb.jdbc.log.console.threshold` | The threshold for console logging. | `ERROR` |

To set these properties, use the `JAVA_TOOL_OPTIONS` environment variables with the following format 
`-D<property-name>=<property-value>`. 

For example:
- In Windows:`set JAVA_TOOL_OPTIONS=-Ddocumentdb.jdbc.log.level=DEBUG`
- In MacOS/Linux: `export JAVA_TOOL_OPTIONS=-Ddocumentdb.jdbc.log.level=DEBUG`
    - Or in DbVisualizer Tools Properties (version 12.1.2 or later):
        - `-Ddocumentdb.jdbc.log.level=DEBUG`
        - `-Ddocumentdb.jdbc.log.console.threshold=ALL`
        - Additionally the following files `slf4j-api.jar` and `slf4j-nop.jar` needs to be removed from the DbVisualizer 
        `lib` folder.
        
- In Tableau, a parameter must be used in the command line or terminal:
    - In Windows: `start "" "c:\program files\Tableau\Tableau [version]\bin\tableau.exe" -DLogLevel=DEBUG`
    - In MacOS: `/Applications/Tableau\ Desktop\[version].app/Contents/MacOS/Tableau -DLogLevel=DEBUG`
    - Tableau logs are located at: `{user.home}/Documents/My Tableau Repository/Logs`