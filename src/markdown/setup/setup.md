# Amazon DocumentDB JDBC Driver Setup

## Topics
- [Prerequisites](#prerequisites)  
    - [DocumentDB Cluster](#documentdb-cluster)
    - [JRE or JDK](#jre-or-jdk) 
    - [DocumentDB JDBC Driver](#documentdb-jdbc-driver)
- [Specifying the Amazon RDS Certificate Authority Certificate File](#specifying-the-amazon-rds-certificate-authority-certificate-file) 
- [Using an SSH Tunnel to Connect to Amazon DocumentDB](#using-a-ssh-tunnel-to-connect-to-amazon-documentdb)
- [Next Steps](#next-steps)
    
## Prerequisites

### DocumentDB Cluster
If you don't already have an Amazon DocumentDB cluster, there are a number of ways to 
[get started](https://docs.aws.amazon.com/documentdb/latest/developerguide/get-started-guide.html). 

Note that DocumentDB is a Virtual Private Cloud (VPC) only service. 
If you will be connecting from a local machine outside the cluster's VPC, you will need to 
create an SSH connection to an Amazon EC2 instance. In this case, launch your cluster using the instructions in 
[Connect with EC2](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-ec2.html). 
See [Using an SSH Tunnel to Connect to Amazon DocumentDB](#using-a-ssh-tunnel-to-connect-to-amazon-documentdb) 
for more information on SSH tunneling and when you might need it.

### JRE or JDK
Depending on your BI application, you may need to ensure a 64-bit JRE or JDK installation version 8 
or later is installed on your computer. You can download the Java SE Runtime Environment 8 
[here](https://www.oracle.com/ca-en/java/technologies/javase-jre8-downloads.html).  

### DocumentDB JDBC Driver
<!-- >TODO: Get link for DocumentDB JDBC driver. -->
Download the DocumentDB JDBC driver [here](https://github.com/aws/amazon-documentdb-jdbc-driver/releases). The driver is packaged as a single JAR file
(e.g., `documentdb-jdbc-1.0-SNAPSHOT-all.jar`).

## Specifying the Amazon RDS Certificate Authority Certificate File
If you are connecting to a TLS-enabled cluster, you may want to specify the Amazon RDS Certificate Authority certificate 
on your connection string. By default, an Amazon RDS Certificate Authority root certificate has been embedded in the 
JDBC driver JAR file which should work when connecting to Amazon DocumentDB clusters using SSL/TLS encryption. However, 
if you want to provide a new Amazon RDS Certificate Authority root certificate, follow the directions below:
1. [Download the root CA certificate](https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem)
2. It is recommended to relocate the file to your user's home directory: `$HOME` for Windows or `~` for MacOS/Linux.
3. Add the `tlsCAFile` option to your [JDBC connection string](connection-string.md). For example: 
   
    ~~~
    jdbc:documentdb://localhost:27017/<database-name>?tlsAllowInvalidHostnames=true&tlsCAFile=rds-ca-2019-root.pem
    ~~~

To determine whether your cluster is TLS-enabled, you can 
[check the value of your cluster's `tls` parameter](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect_programmatically.html#connect_programmatically-determine_tls_value).

## Using a SSH Tunnel to Connect to Amazon DocumentDB
Amazon DocumentDB (with MongoDB compatibility) clusters are deployed within an Amazon Virtual Private Cloud (Amazon VPC). 
They can be accessed directly by Amazon EC2 instances or other AWS services that are deployed in the same Amazon VPC. 
Additionally, Amazon DocumentDB can be accessed by EC2 instances 
or other AWS services in different VPCs in the same AWS Region or other Regions via VPC peering.

However, suppose that your use case requires that you (or your application) access your Amazon DocumentDB resources 
from outside the cluster's VPC. This will be the case for most users not running their application 
on a VM in the same VPC as the DocumentDB cluster. When connecting from outside the VPC, 
you can use SSH tunneling (also known as  _port forwarding_) to access your Amazon DocumentDB resources.

There are two options to create a SSH tunnel:
1. Internally, using the [SSH tunnel options](connection-string.md) (minimally, `sshUser`, `sshHost`, and 
`sshPrivateKeyFile`).
2. Externally, using the `ssh` application. For further information on creating an external SSH tunnel, please refer to
the documentation on [Connecting from Outside an Amazon VPC](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html)


To create an SSH tunnel, you need an Amazon EC2 instance running in the same Amazon VPC as your Amazon DocumentDB 
cluster. You can either use an existing EC2 instance in the same VPC as your cluster or create one. Connecting from 
outside the Amazon VPC using a SSH tunnel will have the following impact on the [JDBC connection string](connection-string.md):
1. If your cluster has Transport Layer Security (TLS) enabled, you will need to add the `tlsAllowInvalidHostNames=true` 
option.
2. As the SSH tunnel is running on your local computer, the host name must be set to `localhost`.
3. If your local port (`-L <local-port>:<cluster-host>:<remote-port>`) configured for the SSH tunnel is not the default
port (27017) for Amazon DocumentDB, ensure the connection string host setting for your SSH tunnel is properly set in the
[JDBC connection string](connection-string.md).
4. The `replicaSet` option is not supported when using a SSH tunnel.

Start an SSH port-forwarding tunnel to the cluster with the following command:

~~~
ssh -i <ssh-key-pair-filename>.pem -N -L <local-port>:<cluster-host>:<remote-port> <ec2-username>@<public-IPv4-DNS-name>
~~~

- The `-L` flag defines the port forwarded to the remote host and remote port. Adding the `-N` flag means do not 
          execute a remote command
- Ensure you have read access permission for the file `<ssh-key-pair-filename>.pem`. To enable read access permission you 
can run the following command: `chmod 400 <ssh-key-pair-filename>.pem`.

This is a prerequisite for connecting to any BI tool running on a client outside your VPC.

Example: Given the following
- `<cluster-host>` = `sample-cluster.node.us-east-1.docdb.amazonaws.com`
- `<public-IPv4-DNS-name>` = `ec2-34-229-221-164.compute-1.amazonaws.com`
- `<local-port>` = `27117`
- `<remote-port>` = `27017`
- `<ssh-key-pair-filename>.pem` = `~/.ssh/ec2Access.pem`
- `<ec2-username> ` = `ubuntu`

    The SSH tunnel command would look like:

    ~~~
    ssh -i ~/.ssh/ec2Access.pem -N -L 27117:sample-cluster.node.us-east-1.docdb.amazonaws.com:27017 ubuntu@ec2-34-229-221-164.compute-1.amazonaws.com
    ~~~

    The [JDBC connection string](connection-string.md) for connecting to the TLS-enabled Amazon DocumentDB cluster with 
    `<database-name>` = `customer` would look like:

    ~~~
    jdbc:documentdb://localhost:27117/customer?tlsAllowedInvalidHostnames=true
    ~~~

For further information on SSH tunneling , please refer to the documentation on
[Connecting from Outside an Amazon VPC.](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html)

## Driver Setup in BI Applications
To learn how to set up the driver in various BI tools, instructions are outlined here for:
1. [Tableau Desktop](tableau-desktop-driver-setup.md) 
2. [DbVisualizer](dbvisualizer-driver-setup.md)
3. [SQuirrel SQL Client](squirrel-sql-client-driver-setup.md) ([Link to product webpage](https://www.dbvis.com/))
4. [Dbeaver](dbeaver-driver-setup.md)
5. [MicroStrategy Developer](microstrategy-developer-driver-setup.md)