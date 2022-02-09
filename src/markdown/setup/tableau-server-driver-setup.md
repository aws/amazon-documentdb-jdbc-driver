### Tableau Server
[Link to product webpage](https://www.tableau.com/products/server). 

#### Adding the Amazon DocumentDB JDBC Driver
[Download](https://github.com/aws/amazon-documentdb-jdbc-driver/releases/latest) the DocumentDB JDBC driver JAR file and copy it to one of these
directories according to your operating system: 
   - **_Windows_**: `C:\Program Files\Tableau\Drivers` 
   - **_Linux_**: `/opt/tableau/tableau_driver/jdbc` 

These directories may need to be created if they do not yet exist. This must be done on each server node.

#### Adding the Tableau connector
1.  [Download](https://github.com/aws/amazon-documentdb-jdbc-driver/releases/latest) the DocumentDB Tableau connector (a TACO file).
2.	For each server node:
      - Create a directory for Tableau connectors if none exists. This needs to be the same path on each machine, and on the same drive that the server is installed on. 
        For example: `C:\Users\Public\Documents\tableau_connectors` on **_Windows_** or `/srv/tableau_connectors` on **_Linux_**
      - Place the downloaded TACO file in this directory.
3.	Set the `native_api.connect_plugins_path` option. For example: `tsm configuration set -k native_api.connect_plugins_path -v C:/Users/Public/Documents/tableau_connectors`
      - If you get a configuration error during this step, try adding the `--force-keys option` to the end of the command.
5.	Apply the pending configuration changes to restart the server: `tsm pending-changes apply`
      - Note: Whenever you add, remove, or update a connector, you must restart the server to see the changes. 
        For information about using TSM to set the options, see [tsm configuration set Options](https://onlinehelp.tableau.com/current/server-linux/en-us/cli_configuration-set_tsm.htm).

For more information, consult the [Tableau documentation](https://tableau.github.io/connector-plugin-sdk/docs/run-taco).