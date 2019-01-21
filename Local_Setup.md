This will help you install all the workshop dependencies on your local workstation.


## For MacOSX 
For MacOSX, many of these should already exist. You'll probably only have to install Scala, Gradle and Luigi manually. 
Consider 
Once you have everything installed, run the verify commands below to double check. 

## For Windows

Use the VM [VM_Setup](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/VM_Setup.md)

## For Linux:

1. Add Java, Scala and Gradle

Java: </br>
`sudo apt install openjdk-8-jdk` </br>
Scala: </br>
`wget www.scala-lang.org/files/archive/scala-2.11.8.deb` </br>
`sudo dpkg -i scala-2.11.8.deb` </br>
Gradle: </br>
`sudo apt install gradle` </br>

2. If not already installed, get Python 2.7 and latest pip. </br>
`sudo apt update` </br>
`sudo apt upgrade` </br>
`sudo apt install python2.7 python-pip` </br>

To verify steps 1 and 2:</br>
`java -version`</br>
Expected output: `openjdk version "1.8.0_191"` or similar 1.8 version.</br> 
`scala -version`</br>
Expected output `Scala code runner version 2.11.8 -- Copyright 2002-2016, LAMP/EPFL` or similar 2.11 version.</br> 
`gradle -version`</br> 
Expected output: `Gradle 3.4.1` or similar 3.4 version.</br> 
`python —version`</br>
Expected output: `Python 2.7.15rc1` or similar version. (2.7.12, 2.17.13, etc.)</br>
`pip —version`</br>
Expected output: `pip 9.0.1 from /usr/lib/python2.7/dist-packages (python 2.7)` or similar version.</br> 
    
    
3. Add luigi. 
`sudo pip install luigi` if using system-install of python. If using a local install like with pyenv don’t use sudo. 
`luigi`
Expected output: `No task specified`

3. Get and unpack Spark.
`wget https://archive.apache.org/dist/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz`
`sudo tar -xzvf spark-2.2.1-bin-hadoop2.7.tgz --directory /opt/`
`sudo mv /opt/spark-2.2.1-bin-hadoop2.7 /opt/spark-2.2.1`

4. Setup bash profile for spark. 
`vi ~/.bash_profile`
Enter the below
{code}
export SPARK_HOME=/opt/spark-2.2.1
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
{code}
`source .bash_profile`


Run the command: `spark-submit`
Expected output `Usage: spark-submit [options] ...` plus many more lines. 