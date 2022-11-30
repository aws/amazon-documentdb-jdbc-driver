# Developer's Guide

## Prerequisites

### IntelliJ IDEA (Community Edition)

This project uses the IntelliJ IDEA (Community Edition). 
Ensure you have the latest version which can be downloaded from 
their [download page](https://www.jetbrains.com/idea/download/).

### Java JDK

This project's driver uses the Java runtime. To develop this project, you'll need to have
a version of the Java JDK. This driver runtime supports a minimum of Java 8 (1.8).
However, for development, you may want to use a more recent LTS version like Java JDK 17.
You can download a version using the IntelliJ IDEA through the menu options:
**File/Project Structure...**, **Project Settings/Project/SDK** and the **Edit** button.

## Integration Testing

By default, integration testing is disabled for local development. 

### Environment

To enable integration testing the following environment variables allow
you to customize the credentials and DocumentDB cluster settings.

| Variable               | Description                                                                                                              | Example                                                                     |
|------------------------|--------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `DOC_DB_USER_NAME`     | This is the DocumentDB user.                                                                                             | `documentdb`                                                                |
| `DOC_DB_PASSWORD`      | This is the DocumentDB password.                                                                                         | `aSecret`                                                                   |
| `DOC_DB_LOCAL_PORT`    | This is the port number used locally via an SSH Tunnel. It is recommend to use a different value than the default 27017. | `27019`                                                                     |
| `DOC_DB_USER`          | This is the user and host of SSH Tunnel EC2 instance.                                                                    | `ec2-user@254.254.254.254`                                                  |
| `DOC_DB_HOST`          | This is the host of the DocumentDB cluster server.                                                                       | `docdb-jdbc-literal-test.cluster-abcdefghijk.us-east-2.docdb.amazonaws.com` |
| `DOC_DB_PRIV_KEY_FILE` | This is the path to the SSH Tunnel private key-pair file.                                                                | `~/.ssh/ec2-literal.pem`                                                    |

### Enabling Integration Testing in the Project

The project file `gradel.properties` contains the property `runRemoteIntegrationTests`. To enable 
integration testing, uncomment the line with the property set to `true`. To disable integration testing,
comment the line.

### SSH Tunnel

To run integration tests, you need to have installed and started an SSH Tunnel to 
the DocumentDB Cluster.

Assuming you have then environment variables setup above, the command line should look like this.
```shell
ssh -f -N -i $DOC_DB_PRIV_KEY_FILE -L $DOC_DB_LOCAL_PORT:$DOC_DB_HOST:27017 $DOC_DB_USER
```

As an example, you can look at how the GitHub workflow starts the SSH Tunnel [here](https://github.com/aws/amazon-documentdb-jdbc-driver/blob/1edd9e21fdcccfe62d366580702f2904136298e5/.github/workflows/gradle.yml#L73).

### Project Secrets

For the purposes of automated integration testing in **GitHub**, this project maintains the value for the environment variables above
as project secrets. See the workflow file [gradle.yml](https://github.com/aws/amazon-documentdb-jdbc-driver/blob/1edd9e21fdcccfe62d366580702f2904136298e5/.github/workflows/gradle.yml)
