# Maintaining the Known Hosts File

## Introduction

When working with an Amazon DocumentDB cluster in a Virtual Private Cloud (**VPC**) from the 
internet, an SSH port forwarding tunnel is required to reach the cluster inside the VPC.

There are two ways to create an SSH tunnel:
1. Using the command line SSH application [using an SSH tunnel to connect to DocumentDB](setup.md#using-an-ssh-tunnel-to-connect-to-amazon-documentdb)
2. Using the connection string [options](connection-string.md).

In either case, if you want to take advantage of strict checking of the SSH tunnel host key, 
you will need to maintain the **_known hosts_** file.

## Using the SSH Application to Add Entries

By default, the SSH command, will prompt you the first time it encounters the SSH tunnel IP 
address or hostname.

```text
$ ssh -i ~/.ssh/private-key.pem -N -L 27019:docdb.cluster-xxxx.us-east-2.docdb.amazonaws.com:27017 ec2-user@254.254.254.254
The authenticity of host '254.254.254.254 (254.254.254.254)' can't be established.
ED25519 key fingerprint is SHA256:vHtm ... .
This key is not known by any other names
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
```

By typing `yes` and pressing **Enter**, the SSH application will create one or more entries in the 
`~/.ssh/known_hosts` for the SSH tunnel IP address or hostname.



## Using the SSH-KEYSCAN Application to Add Entries

If you don't intend on using the Amazon DocumentDB JDBC driver connection properties to create an
internal SSH port-forwarding tunnel, then you can use the
[SSH-KEYSCAN application](https://man.openbsd.org/ssh-keyscan.1) to maintain the **known hosts**
file.

In this example we'll create three entries for each hash algorithm (`ecdsa`, `ed25519`, and `rsa`
for the host `123.454.321.123` and appending the entries to the end of the `known_hosts` file.

```text
$ ssh-keyscan -t ecdsa,ed25519,rsa 123.454.321.123 >> ~/.ssh/known_hosts
```

## Installing SSH and SSH-KEYSCAN
### Windows

To obtain and install SSH on Windows follow these
[instructions](https://docs.microsoft.com/en-us/windows-server/administration/openssh/openssh_overview).
Note: You'll only need the **OpenSSH Client**.

### Mac OS

Mac OS X already has the SSH client installed and available from the command prompt.