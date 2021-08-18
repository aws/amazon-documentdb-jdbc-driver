# Troubleshooting Guide

## Connection Issues

### Invalid host or port number

#### What to look for:

-  `java.net.ConnectException: Connection refused: connect`

##### Tableau Connection Error
    
```text
An error occurred while communicating with DocumentDB by Amazon

Bad Connection: Tableau could not connect to the data source.

Error Code: FAB9A2C5

Timed out after 30000 ms while waiting to connect. 
Client view of cluster state is {type=UNKNOWN, servers=[{address=localhost:27017, type=UNKNOWN, state=CONNECTING, 
exception={com.mongodb.MongoSocketOpenException: Exception opening socket},
 caused by {java.net.ConnectException: Connection refused: connect}}]
 
Unable to connect to the DocumentDB by Amazon server "localhost". 
Check that the server is running and that you have access privileges to the requested database.

Connector Class: documentdbjdbc, Version: ...
```

##### DbVisualizer Connection Error

```text
An error occurred while establishing the connection:

Long Message:

Timed out after 30000 ms while waiting to connect. 
Client view of cluster state is {type=UNKNOWN, servers=[{address=localhost:27017, type=UNKNOWN, state=CONNECTING, 
exception={com.mongodb.MongoSocketOpenException: Exception opening socket},
 caused by {java.net.ConnectException: Connection refused: connect}}]
```

#### What to do:
This problem suggests that you are unable to reach the DocumentDB cluster. Most commonly, this is 
because you are outside the cluster's VPC. To connect to an Amazon DocumentDB cluster from outside 
an Amazon VPC, you can use an SSH tunnel. The driver allows users to set up an SSH tunnel manually 
or pass additional parameters in the connecting string so that the driver can create one automatically. 
See [Using an SSH Tunnel to Connect to Amazon DocumentDB](../setup/ssh-tunnel.md) for more information 
on both methods. 

If this is still an issue after having setup the SSH tunnel, double-check the values of *Port* or 
*Hostname*.

If you have manually set up an SSH tunnel:
1. Ensure the *Port* number in your connection string matches the local port number configured when using an SSH tunnel. 
   In this example, the local port is **27019** not 27017:
      ```
      ssh -i "ec2Access.pem" -L 27019:sample-cluster.node.us-east-1.docdb.amazonaws.com:27017 ec3-user@ec2.compute-1.amazonaws.com -N
      ```
1. Ensure the *Hostname* in your connection string is set to `localhost`.

If you are using the automatically set up SSH tunnel:  
1. Ensure the *Port* number in your connection matches the cluster's port number. Typically, this is `27017`. 
1. Ensure the *Hostname* in your connection string is set to the cluster hostname and **not** `localhost`.

If the issue is unrelated to SSH tunneling, 
double check other parameters in your [connection string]()
and make sure the cluster is running and available to connect to.

### Internally Embedded Certificate Authority Issue       

#### What to look for:

-  `unable to find valid certification path to requested target`
-  `Server's certificate with common name ... is not trusted`

##### Tableau Certificate Error

```text
An error occurred while communicating with DocumentDB by Amazon

Bad Connection: Tableau could not connect to the data source.

Error Code: FAB9A2C5

Timed out after 30000 ms while waiting to connect. 
Client view of cluster state is {type=UNKNOWN, servers=[{address=localhost:27019, type=UNKNOWN, state=CONNECTING, 
exception={com.mongodb.MongoSocketWriteException: Exception sending message},
 caused by {javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target},
 caused by {sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target}, 
 caused by {sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target}}]
 
Unable to connect to the DocumentDB by Amazon server "localhost". 
Check that the server is running and that you have access privileges to the requested database.

Connector Class: documentdbjdbc, Version: ...
```

##### DbVisualizer Certificate Error

```text
Server's certificate with common name ... is not trusted.

The reason a server is not trusted is most likely due to missing certificate authorities in the truststore. 
The online security resources may give a pointer how to fix this.
```

#### What to do:

1. Download the latest root certificate authority file. 
   See [Specifying the Amazon RDS Certificate Authority Certificate File](../setup/amazon-ca-certs.md). 
1. Copy the file to your home directory.
1. Provide the root certificate file name in the connection. 
   1. Tableau: *TLS Certificate Authority File (Optional)* : `~/rds-ca-2019-root.pem`
   1. DbVisualizer: `jdbc:documentdb://localhost:27017/test?tls=true&tlsAllowInvalidHostnames=true&tlsCAFile=~/rds-ca-2019-root.pem`
    
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
## Query Issues

#### What to look for: 

#### What to do: 
1. Double check that the identifiers using