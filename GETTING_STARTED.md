# Getting Started as a Developer
This document provides instructions on how to install and set up Java (JDK 1.8) and an IDE (IntelliJ IDEA) on your computer to be able compile the project source code and run DocumentDB tests.

## Table of Contents
1. [Instructions](#instructions)
    1. [Installing Java](#installing-java)
    2. [Installing IntelliJ](#installing-intellij-idea)
    3. [Cloning Source Code onto Local Computer](#cloning-source-code-onto-local-computer)
    4. [Building Project with Gradle](#building-project-with-gradle)
    5. [Setting Up Environment to Run DocumentDB Tests](#setting-up-environment-to-run-documentdb-tests)
        1. [Connect to a DocumentDB Cluster Using an SSH Tunnel](#connect-to-a-documentdb-cluster-using-an-ssh-tunnel)
            1. [Prerequisites](#prerequisites)
            2. [References](#references)
            3. [Creating an EC2 Instance in a VPC](#creating-an-ec2-instance-in-a-vpc)
            4. [Create a DocumentDB Cluster and an SSH Tunnel](#create-a-documentdb-cluster-and-an-ssh-tunnel)
            5. [Connect with TLS](#connect-with-tls)
            6. [Connect Programmatically](#connect-programmatically)
                1. [Without TLS](#without-tls)
                2. [With TLS](#with-tls)
        2. [Setting Up Environment Variables](#setting-up-environment-variables)
        3. [Bypass Testing DocumentDB](#bypass-testing-documentdb)
2. [Troubleshooting](#troubleshooting)
    1. [Issues with JDK](#issues-with-jdk)
    2. [Error Connecting to DocumentDB Instance (Connection Time Out)](#error-connecting-to-documentdb-instance-connection-time-out)
            
## Instructions
### Installing Java
1. Go to this [page](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to download Java SE Development Kit 8 (JDK 1.8) for your system and follow installation steps (you need to have or create an Oracle account to download).
2. Confirm correct version (1.8) of Java has been installed by running the command `java -version` or `javac -version` in the command line or terminal.

### Installing IntelliJ IDEA
1. Go to this [page](https://www.jetbrains.com/idea/download/) to download IntelliJ IDEA Community Edition for your system and follow the installation steps (click next and continue with default settings)
2. The plugins *SpotBugs* and *Gradle* need to be installed in the IDE which can be done via the top menu toolbar under *IntelliJ IDEA → Preferences → Plugins* (search in the *Marketplace*).

### Cloning Source Code onto Local Computer
1. If you do not have a SSH key associated with your GitHub account, follow the steps on this [page](https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account) to add a new SSH key to your GitHub account.
2. In the Amazon [documentdb-jdbc](https://github.com/aws/amazon-documentdb-jdbc-driver) repository, copy the SSH URL to your clipboard when you click the download code button as shown in the image below. 

   ![Copy SSH Key from GitHub Repository](src/markdown/images/Clone-Repository.png)

3. Open the command line or terminal and navigate to the folder/directory where you would like to clone the repository to and run the following command `git clone X` where `X` is the SSH URL to the repository copied to your clipboard in the above step.

### Building Project with Gradle
1. In IntelliJ, open Gradle tool window: navigate to the top menu toolbar *View → Tool Windows → Gradle*.
2. In the Gradle tool window, build the project by double-clicking on gradle task *documentdb-jdbc → Tasks → shadow → shadowJar*.
3. After build and compilation, a .jar file should be created in your repository in the subfolder *documentdb-jdbc/build/libs*.

   ![Gradle Build](src/markdown/images/Gradle-Build.png)

### Setting Up Environment to Run DocumentDB Tests
This section describes how to connect to a DocumentDB cluster using an SSH tunnel and details the setup needed to run tests against the DocumentDB instance.

#### Connect to a DocumentDB Cluster Using an SSH Tunnel
Since DocumentDB is currently a VPC-only service, you need to set up an SSH tunnel using an EC2 instance running in the same VPC as your DocumentDB cluster (Note that SSH tunnelling will not work for connecting in replica set mode).

##### Prerequisites
1. Mongo shell (comes with MongoDB server installation but can be downloaded separately)
    1. Download Mongo shell on macOS using HomeBrew by running following commands in the terminal:
        
        ~~~
        brew tap mongodb/brew
        brew install mongodb-community-shell
        ~~~
       
2. AWS account (or a downloaded private key granting access to the cluster)

##### References
1. [Walkthrough on creating an EC2 instance and SSH-ing into it](https://medium.com/serverlessguru/creating-an-aws-ec2-instance-d5cf332fdb0c)
2. [Walkthrough on creating a documentDB cluster with SSH tunnel](https://medium.com/serverlessguru/creating-an-aws-documentdb-cluster-cf658052cef0)

##### Creating an EC2 Instance in a VPC
If you already have an instance of an Amazon EC2 running in the same VPC as your DocumentDB cluster, you can skip these steps as long as you have the PEM file for the instance's key pair.
1. Navigate to AWS EC2 Dashboard and select **Launch an Instance**. 
2. Select the **Amazon Linux 2 AMI (HVM), SSD Volume Type** as the machine image and **t2.micro** as the instance type. Leave everything as default and launch the instance.  
   1. **NOTE:** This will create the instance in your default VPC. You can select a different VPC if necessary but should remember to create your DocumentDB cluster there as well. 
3. You will be prompted to provide or generate a key pair for the instance. Create a new key pair with a meaningful name and download it. Make sure to keep track of this file.
4. Give the instance a meaningful name and wait for the instance to have a state of running.
5. Move the key pair file (a PEM file) into your ssh directory.

   ~~~
   mv downloads/<key pair name>.pem ~/.ssh/
   ~~~
   
6. Modify its permissions.

   ~~~
   chmod 400 ~/.ssh/<key pair name>.pem
   ~~~
   
7. SSH into the instance to verify the setup!

   ~~~
   ssh -i ~/.ssh/<key pair name>.pem ec2-user@<public IPv4 DNS name>
   ~~~
   
   You can find the public dns name of your instance from the EC2 dashboard. Select your instance and copy the public IPv4 DNS from the instance memory.

   ![EC2 Instance](src/markdown/images/EC2-instance.png)

   Example output:
   
   ![Example output SSH Tunnel to EC2 instance](src/markdown/images/Example-output-SSH-to-EC2.png)
   
##### Create a DocumentDB Cluster and an SSH Tunnel
1. Navigate to the DocumentDB dashboard. From the sidebar, select **Parameter Groups** and click **Create**. 
2. Flip the TLS and TTL Monitor switches to disabled. You can leave the TLS switch as enabled if you want to connect with TLS right away.
3. From the sidebar, select **Clusters** and click **Create**. 
4. Give the instance a meaningful identifier and choose **t3.medium** as the instance class and **1** as the number of instances.

   ![Create Amazon DocumentDB Cluster](src/markdown/images/Create-AMZNDocDB-Cluster.png)
   
5. Fill in the authentication section and take note of the master username and password entered. You will use these to connect later.
6. Under **Advanced Settings**:
    1. Choose the parameter group created in step 1 as the cluster parameter group.
    
       ![Cluster Options](src/markdown/images/Cluster-Options.png)
       
    2. Choose the VPC used during EC2 instance creation as the VPC.
   
       ![Network Settings](src/markdown/images/Network-Settings.png)
       
7. Create the cluster and wait for it to finish spinning up.
8. Once the cluster is available, we can create the SSH tunnel using our EC2 instance. Use the same key pair file and public DNS name used to SSH into the EC2 instance. Use the cluster endpoint from the configuration tab of your cluster.

   ~~~
   ssh -i ~/.ssh/<key pair>.pem -N -L 27017:<cluster endpoint>:27017 ec2-user@<public IPv4 DNS name>
   ~~~
   ![Cluster Endpoint](src/markdown/images/Cluster-Endpoint.png)
   
9. Open another terminal window and connect to the cluster using the `mongo` shell. Note that we will use localhost rather than the cluster endpoint since we have set up the SSH tunnel.

   ~~~
   mongo --host 127.0.0.1:27017 --username <master username> --password <master password>
   ~~~
   
##### Connect with TLS
When connecting to a TLS-enabled cluster you can follow the same steps to set up an SSH tunnel but will need to also download the Amazon DocumentDB Certificate Authority (CA) file before trying to connect.
1. Download the CA file.

   ~~~
   wget https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem
   ~~~
   
2. Set up the SSH tunnel. See step 3 in section [Setting Up Environment Variables](#setting-up-environment-variables) for command to start SSH port-forwarding tunnel. 

3. Open another terminal window and connect using the `mongo` shell. Note that since we are using an SSH tunnel to access the cluster from localhost, the server certificate does not match the hostname. It is expecting a hostname like `sample-cluster.xxxxxxxxxxxx.us-east-1.docdb.amazonaws.com:27017` not `127.0.0.1:27017`. We have to pass the `--tlsAllowInvalidHostnames` flag or the handshake will fail.

   ~~~
   mongo --host 127.0.0.1:27017 --username <master username> --password <master password> --tls --tlsCAFile rds-combined-ca-bundle.pem --tlsAllowInvalidHostnames 
   ~~~
   
##### Connect Programmatically 
###### Without TLS
Connecting without TLS is very straightforward. We essentially follow the same steps as when connecting using the `mongo` shell. 
1. Setup the SSH tunnel. See step 3 in section [Setting Up Environment Variables](#setting-up-environment-variables) for command to start SSH port-forwarding tunnel.
2. Create a test or simple main to run.
3. Use either the Driver Manager, Data Source class or Connection class to establish a connection to `localhost:27017`. Make sure to set the hostname, username, password and target database. The target database can be anything as long as it is a valid MongoDB database name (no spaces, no special chars). See example directly using the connection class: 

   ~~~js
   @Test
   public void sshTunnelTestWithoutSSL() throws SQLException {
       final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
       properties.setHostname("localhost:27017");
       properties.setUser("<user name>");
       properties.setPassword("<password>");
       properties.setDatabase("<target database name>");
       final DocumentDbConnection connection = new DocumentDbConnection(properties);
       connection.close();
   }
   ~~~
   
###### With TLS
Connecting with TLS programmatically is slightly different from how we did it with the `mongo` shell. We need to set up a trust store with the CA certificate. 
1. Create a test or simple main to run.  
2. Use either the Driver Manager, Data Source class or Connection class to establish a connection to `localhost:27017`. Make sure to set hostname, username, password, target database and tls parameters. The target database can be anything as long as it is a valid MongoDB database name (no spaces, no special chars). See example directly using the connection class:
   
   ~~~js
   @Test
   public void sshTunnelTestWithSSL() throws SQLException {
       final DocumentDbConnectionProperties properties = new DocumentDbConnectionProperties();
       properties.setHostname("localhost:27017");
       properties.setTlsEnabled("true");
       properties.setTlsAllowInvalidHostnames("true");
       properties.setUser("<user name>");
       properties.setPassword("<password>");
       properties.setDatabase("<target database name>");
       final DocumentDbConnection connection = new DocumentDbConnection(properties);
       connection.close();
   }
   ~~~

#### Setting Up Environment Variables
1. Create and set the Environment Variables:

   ~~~
   DOC_DB_USER_NAME=<secret username>
   DOC_DB_PASSWORD=<secret password>
   DOC_DB_LOCAL_PORT=27019
   DOC_DB_USER=ec2-user@<public IPv4 DNS name>
   DOC_DB_HOST=<cluster endpoint>
   DOC_DB_PRIV_KEY_FILE=~/.ssh/<key pair name>.pem
   ~~~

2. Ensure the private key file <key pair name>.pem is in the location set by the environment variable `DOC_DB_PRIV_KEY_FILE`.
3. Start an SSH port-forwarding tunnel:

   ~~~
   ssh [-f] -N -i ~/.ssh/<key pair name>.pem -L $DOC_DB_LOCAL_PORT:$DOC_DB_HOST:27017 $DOC_DB_USER
   ~~~
   
   (Note: use the `-f` option if you want to have the SSH run in the background)
   
#### Bypass Testing DocumentDB
1. Modify the */gradle.properties* file in the source code and uncomment the following line: `runRemoteIntegrationTests=false`

## Troubleshooting
### Issues with JDK
1. Confirm project SDK is Java Version 1.8 via the IntelliJ top menu toolbar under *File → Project Structure → Platform Settings -> SDK* and reload the JDK home path by browsing to the path and click *apply* and *ok*. Restart IntelliJ IDEA.

   ![Setting JDK Home Path](src/markdown/images/Project-Structure-SDK.png)

2. If above step does not solve the issue, try to see if reinstalling JDK 1.8 fixes the problem.
    1. For Mac OS/Linux, uninstall JDK using commands below, restart your computer and install JDK 1.8 as describe in the [Instructions](#instructions) section above.
     
       ~~~
       sudo rm -rf /Library/Internet\ Plug-Ins/JavaAppletPlugin.plugin
       sudo rm -rf /Library/Java/JavaVirtualMachines
       sudo rm -rf /Library/Application\ Support/Oracle/Java
       sudo rm -rf /Library/PreferencePanes/JavaControlPanel.prefPane
       ~~~

### Error Connecting to DocumentDB Instance (Connection Time Out)
1. DocumentDB instance and EC2 should be deployed in the same region and use the same VPC. Check Region and VPC.
2. Check Security Policy for EC2 instance allows inbound connections and ensure it is configured as shown in the picture below. Go to EC2 Dashboard → **Network & Security** Group in the left menu → **Security** Group.

   ![Security Policy for EC2 Instance](src/markdown/images/Security-Policy-EC2-Instance.png)
   
   