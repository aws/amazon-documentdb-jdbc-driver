# Specifying the Amazon RDS Certificate Authority Certificate File

By default, an Amazon RDS Certificate Authority root certificate is embedded in the JDBC driver JAR 
file which should work when connecting to Amazon DocumentDB clusters using SSL/TLS encryption. 
However, if you want to provide a new Amazon RDS Certificate Authority root certificate, follow the
direction below.

The certificate is distributed 
[unbundled here](https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem).

1. [Download the root CA certificate](https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem)
1. We recommend you relocate the file into your user's home directory:
   1. Window: `$HOME`
   1. MacOS/Linus: `~`
1. Add the `tlsCAFile` option to your [JDBC connection string](connection-string.md).
   For example, `jdbc:documentdb://localhost:27117/customer?tlsAllowInvalidHostnames=true&tlsCAFile=rds-ca-2019-root.pem`
