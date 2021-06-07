# Using an SSH Tunnel to Connect to Amazon DocumentDB

If your Amazon DocumentDB cluster is deployed within an Amazon Virtual Private Cloud (Amazon VPC),
and you want to access it from outside the VPC, you'll need to create an SSH tunnel to access your
Amazon DocumentDB resources.

For further information, please refer to the documentation on
[Connecting from Outside an Amazon VPC.](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html)

## The JDBC Connection String When Using an SSH Tunnel

Connecting to your Amazon DocumentDB cluster from outside the Amazon VPC using an SSH tunnel will
have the following impact on the JDBC connection string.

1. If your cluster has Transport Layer Security (TSL) enabled, you'll need to add the
   `tlsAllowInvalidHostnames=true` option.
1. As the SSH tunnel is running on your local computer, the host name must be set to `localhost`.
1. If your local port (`-L <local-port>:<cluster-host>:<remote-port>`) configured for the SSH tunnel
   is not the default port (27017) for Amazon DocumentDB, ensure the connection string host setting
   is for your SSH tunnel is properly set in the JDBC connection string.
1. The `replicaSet` option is not supported when using an SSH tunnel.

For example, the SSH tunnel command for an Amazon DocumentDB cluster running on
`sample-cluster.node.us-east-1.docdb.amazonaws.com` and running through a tunnel on
`ec2-34-229-221-164.compute-1.amazonaws.com`, where the local port is set to `27117`
would look like following.

```
ssh -i "ec2Access.pem" -L 27117:sample-cluster.node.us-east-1.docdb.amazonaws.com:27017 ubuntu@ec2-34-229-221-164.compute-1.amazonaws.com -N 
```

The connection string for connecting to the TLS-enabled Amazon DocumentDB cluster with database
named `customer` would look like the following.

```
jdbc:documentdb://localhost:27117/customer?tlsAllowInvalidHostnames=true
```
