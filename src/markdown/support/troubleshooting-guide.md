# Troubleshooting Guide

## Connection Issues

### Invalid host or port number

#### What to look for:

1. `java.net.ConnectException: Connection refused: connect`

#### Tableau Connection Error Issue
    
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

#### DbVisualizer Error

```text
An error occurred while establishing the connection:

Long Message:

Timed out after 30000 ms while waiting to connect. 
Client view of cluster state is {type=UNKNOWN, servers=[{address=localhost:27017, type=UNKNOWN, state=CONNECTING, 
exception={com.mongodb.MongoSocketOpenException: Exception opening socket},
 caused by {java.net.ConnectException: Connection refused: connect}}]
```

1. To connect to an Amazon DocumentDB cluster from outside an Amazon VPC, 
  you can use an SSH tunnel. For more information, see [Connecting to an 
  Amazon DocumentDB Cluster from Outside an Amazon VPC](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html). 
  Additionally, if your development environment is in a different Amazon VPC, you can also use 
  VPC Peering and connect to your Amazon DocumentDB cluster from another Amazon VPC in the 
  same region or a different region.
1. Also see [Using an SSH Tunnel to Connect to Amazon DocumentDB](../setup/setup.md#using-a-ssh-tunnel-to-connect-to-amazon-documentdb).
1. Ensure the *Port* number matches the local port number configured when using an SSH tunnel.
1. Ensure the *Hostname* is set to `localhost` when using an SSH tunnel.
      
### Internally Embedded Certificate Authority Issue       

#### What to look for:

1. `unable to find valid certification path to requested target`
1. `Server's certificate with common name ... is not trusted`

#### Tableau Certificate Error

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

#### DbVisualizer Certificate Error

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
   1. Tableau: *TLS Certificate Authority File (Optional)* : `~/rds-ca-2019-root.pem`
   1. DbVisualizer: `jdbc:documentdb://localhost:27017/test?tls=true&tlsAllowInvalidHostnames=true&tlsCAFile=~/rds-ca-2019-root.pem`

## Schema Issues

### Schema Out of Date

#### What to look for: 
1. Cannot find new collection(s) as tables in SQL schema.
1. Cannot find new document field(s) as columns in SQL table schema.

#### What to do:

1. To update the SQL schema for your database, run the command line interface to 
[generate a new schema](../schema/manage-schema-cli.md).
1.  For example:
    ```text
    java -jar document-db-1.0.SNAPSHOT-all.jar --generate-new \
    --server localhost:27019 --database test -u ajones --tls --tls-allow-invalid-hostnames
    ```
