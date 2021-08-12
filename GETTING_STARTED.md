# Getting Started as a Developer
This document provides instructions on how to install and setup Java (JDK 1.8) and an IDE (IntelliJ IDEA) on your computer to be able run and compile the project source code. 

## Instructions
### Installing Java (JDK 1.8)
1. Go to this [page](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to download Java SE Development Kit 8 (JDK 1.8) for your system and follow installation steps (you need to have or create an Oracle account to download).
2. Confirm correct version (1.8) of Java has been installed by running the command `java -version` or `javac -version` in the command line or terminal.

### Installing IntelliJ IDEA
1. Go to this [page](https://www.jetbrains.com/idea/download/) to download IntelliJ IDEA Community Edition for your system and follow the installation steps (click next and continue with default settings)
2. The plugins *SpotBugs* and *Gradle* need to be installed in the IDE which can be done via the top menu toolbar under *IntelliJ IDEA → Preferences → Plugins* (search in the *Marketplace*).

### Copying/Cloning Source Code onto Local Computer
1. If you do not have a SSH key associated with your GitHub account, follow the steps on this [page](https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account) to add a new SSH key to your GitHub account.
2. In the Amazon [documentdb-jdbc](https://github.com/aws/amazon-documentdb-jdbc-driver) repository, copy the SSH URL to your clipboard when you click the download code button as shown in the image below. 

![Copy SSH Key from GitHub Repository](src/markdown/images/Clone-Repository.png)

3. Open the command line or terminal and navigate to the folder/directory where you would like to clone the repository to and run the following command `git clone X` where `X` is the SSH URL to the repository copied to your clipboard in the above step.

### Building Project with Gradle
1. In IntelliJ, open Gradle tool window: navigate to the top menu toolbar *View → Tool Windows → Gradle*.
2. In the Gradle tool window, build the project by double-clicking on gradle task *documentdb-jdbc → Tasks → shadow → shadowJar*.
3. After build and compilation, a .jar file should be created in your repository in the subfolder *documentdb-jdbc/build/libs*.

![Gradle Build](src/markdown/images/Gradle-Build.png)

## Troubleshooting
### Issues with JDK
1. Confirm project SDK is Java Version 1.8 via the IntelliJ top menu toolbar under *File → Project Structure → Platform Settings -> SDK* and reload the JDK home path by browsing to the path and click *apply* and *ok*. Restart IntelliJ IDEA.

![Setting JDK Home Path](src/markdown/images/Project-Structure-SDK.png)

3. If above step does not solve the issue, try to see if reinstalling JDK 1.8 fixes the problem.
    1. For Mac OS/Linux, uninstall JDK using commands below, restart your computer and install JDK 1.8 as describe in the [Instructions](#instructions) section above.
     
  ~~~
    sudo rm -rf /Library/Internet\ Plug-Ins/JavaAppletPlugin.plugin
    sudo rm -rf /Library/Java/JavaVirtualMachines
    sudo rm -rf /Library/Application\ Support/Oracle/Java
    sudo rm -rf /Library/PreferencePanes/JavaControlPanel.prefPane
  ~~~