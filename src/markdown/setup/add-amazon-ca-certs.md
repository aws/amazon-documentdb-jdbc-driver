# Adding Amazon RDS Certificate Authority Bundle

To connect to Amazon DocumentDB clusters using SSL/TLS encryption,
you'll need to install Amazon RDS CA root and intermediate certificates.

The certificates are 
[distributed bundled or unbundled](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html).

## Windows

To install the Amazon RDS CA bundle on Windows, follow these steps.

1. [Download](https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.p7b) the certificate 
   bundle PKCS7 file (with .P7B extension).
1. Open either the "Manage computer certificates" (requires elevation) or "Manage user certificates"
   console.
1. If using "Manage computer certificates" ... 
    1. Navigate the tree **Certificates - Local Computer > Trusted Root Certification Authorities > 
       Certificates**.
1. If using "Manage user certificates" ...
    1. Navigate the tree **Certificates - Current User >
       Trusted Root Certification Authorities > Certificates**.
1. Navigate the menu path **Action > All Tasks > Import**
1. Click **Next**.
1. Click **Browse** to select the certificate bundle file.
1. Navigate to where the P7B file was downloaded. Select the file and click **Open**.
1. Click **Next**.
1. Ensure Place all certificates in the following store: 
   **"Certificate Store: Trusted Root Certification Authorities"**. 
   Click **Next**.
1. Click **Finish**.
1. Click **OK**.

## MacOS

To install a custom certificate on a Mac, follow these steps to import the certificate into the 
"System" keychain.

1. [Download the root CA certificate](https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem) 
   and one of the [intermediate certificates](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html). 
   The intermediate certificate to download is dependent on the AWS region your cluster is deployed in. 
   For example, for a cluster in **US East(N. Virginia)**, the certificate to download would be `rds-ca-2019-us-east-1.pem`.
1. [Open](https://support.apple.com/guide/keychain-access/add-certificates-to-a-keychain-kyca2431/mac)
   the **Keychain Access** app on your Mac.
1. Select the **System** keychain. 
1. Import the certificate into the "System" keychain (not "System Roots").
1. Enable trust as follows:
    1. In the **Keychain App**, right-click the new certificate.
    1. Select **Get Info**.
    1. In the dialog, open the **Trust** section, and then select 
       **When using this certificate always trust**.
       
1. Repeat steps 4. and 5. for each downloaded certificate.

## Linux

For importing Amazon RDS CA certificate bundle, follow the [directions
provided by Tableau](https://help.tableau.com/current/pro/desktop/en-us/jdbc_ssl_config.htm).