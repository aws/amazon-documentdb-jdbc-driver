# Amazon DocumentDB JDBC Driver Setup

## Topics

- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)  
    - [DocumentDB Cluster](#documentdb-cluster)
    - [JRE or JDK Installation](#jre-or-jdk-installation) 
    - [Download the DocumentDB JDBC Driver](#download-the-documentdb-jdbc-driver)
  - [Specifying the Amazon RDS Certificate Authority Certificate File](#specifying-the-amazon-rds-certificate-authority-certificate-file) 
  - [Using an SSH Tunnel to Connect to Amazon DocumentDB](#using-an-ssh-tunnel-to-connect-to-amazon-documentdb)
  - [Next Steps](#next-steps)
    
## Getting Started

### Prerequisites

#### DocumentDB Cluster

If you don't already have an Amazon DocumentDB cluster, there are a number of ways to 
[get started](https://docs.aws.amazon.com/documentdb/latest/developerguide/get-started-guide.html). 

Note that DocumentDB is a Virtual Private Cloud (VPC) only service. 
If you will be connecting from a local machine outside the cluster's VPC, you will need to 
create an SSH connection to an Amazon EC2 instance. In this case, launch your cluster using the instructions in 
[Connect with EC2](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-ec2.html). 
See [Using an SSH Tunnel to Connect to Amazon DocumentDB](#using-an-ssh-tunnel-to-connect-to-amazon-documentdb) 
for more information on ssh tunneling and when you might need it.

#### JRE or JDK Installation

Depending on your BI application, you may need to ensure a 64-bit JRE or JDK installation version 8 
or later is installed on your computer. You can download the Java SE Runtime Environment 8 
[here](https://www.oracle.com/ca-en/java/technologies/javase-jre8-downloads.html).  

#### Download the DocumentDB JDBC Driver

<!-- >TODO: Get link for DocumentDB JDBC driver. -->
Download the DocumentDB JDBC driver [here](https://github.com/aws/amazon-documentdb-jdbc-driver/releases). The driver is packaged as a single JAR file
(e.g., `documentdb-jdbc-1.0-SNAPSHOT-all.jar`).

### Specifying the Amazon RDS Certificate Authority Certificate File

If you are connecting to a TLS-enabled cluster, you may want to 
[specify the Amazon RDS Certificate Authority certificate](amazon-ca-certs.md) on your connection string.

To determine whether your cluster is TLS-enabled, you can 
[check the value of your cluster's `tls` parameter](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect_programmatically.html#connect_programmatically-determine_tls_value).

### Using an SSH Tunnel to Connect to Amazon DocumentDB

Amazon DocumentDB (with MongoDB compatibility) clusters are deployed within an Amazon Virtual Private Cloud (Amazon VPC). 
They can be accessed directly by Amazon EC2 instances or other AWS services that are deployed in the same Amazon VPC. 
Additionally, Amazon DocumentDB can be accessed by EC2 instances 
or other AWS services in different VPCs in the same AWS Region or other Regions via VPC peering.

However, suppose that your use case requires that you (or your application) access your Amazon DocumentDB resources 
from outside the cluster's VPC. This will be the case for most users not running their application 
on a VM in the same VPC as the DocumentDB cluster. When connecting from outside the VPC, 
you can use SSH tunneling (also known as  _port forwarding_) to access your Amazon DocumentDB resources.

To create an SSH tunnel, you need an Amazon EC2 instance running in the same Amazon VPC as your Amazon DocumentDB cluster. You can either use an existing EC2 instance in the same VPC as your cluster or create one.

You can set up an SSH tunnel to the Amazon DocumentDB cluster `sample-cluster.node.us-east-1.docdb.amazonaws.com` by running the following command on your local computer. The `-L` flag is used for forwarding a local port.

```
ssh -i "ec2Access.pem" -L 27017:sample-cluster.node.us-east-1.docdb.amazonaws.com:27017 ubuntu@ec2-34-229-221-164.compute-1.amazonaws.com -N 
```

This is a prerequisite for connecting to any BI tool running on a client outside your VPC. Once you run the step above you can move on to the next steps for the BI tool of your choice.

For further information on SSH tunneling , please refer to the documentation on
[Connecting from Outside an Amazon VPC.](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html)

## Next Steps

To learn how to set up and work with various BI tools, see the 
[BI Tool Setup documentation](bi-tool-setup.md) for more detail.