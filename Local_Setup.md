This will help you install all the workshop dependencies on your local workstation.


## For MacOSX 
For MacOSX, many of these should already exist. You'll probably only have to install Scala, Gradle and Luigi manually. 
Consider 
Once you have everything installed, run the verify commands below to double check. 

## For Windows

Use the VM [VM_Setup](https://github.com/bfemiano/song_plays_workshop_tutorial/blob/master/VM_Setup.md)

## For Linux:

1. Add Java, Scala and Gradle

Java: 
    `sudo apt install openjdk-8-jdk`
Scala: 
    {{wget www.scala-lang.org/files/archive/scala-2.11.8.deb}}
    {{sudo dpkg -i scala-2.11.8.deb}}
Gradle: 
    {{sudo apt install gradle}}

2. If not already installed, get Python 2.7 and latest pip. 
{{sudo apt update}}
{{sudo apt upgrade}}
{{sudo apt install python2.7 python-pip}}

To verify steps 1 and 2:
    {{java -version}} 
    Expected output: {{openjdk version "1.8.0_191"}} or similar 1.8 version. 
    {{scala -version}}
    Expected output {{Scala code runner version 2.11.8 -- Copyright 2002-2016, LAMP/EPFL}} or similar 2.11 version. 
    {{gradle -version}} 
    Expected output: {{Gradle 3.4.1}} or similar 3.4 version. 
    {{python —version}}
    Expected output: {{Python 2.7.15rc1}} or similar version. (2.7.12, 2.17.13, etc.)
    {{pip —version}}
    Expected output: {{pip 9.0.1 from /usr/lib/python2.7/dist-packages (python 2.7)}} or similar version. 
    
    
3. Add luigi. 
{{sudo pip install luigi}} if using system-install of python. If using a local install like with pyenv don’t use sudo. 
{{luigi}}
Expected output: {{No task specified}}

3. Get and unpack Spark.
{{wget https://archive.apache.org/dist/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz}}
{{sudo tar -xzvf spark-2.2.1-bin-hadoop2.7.tgz --directory /opt/}}
{{sudo mv /opt/spark-2.2.1-bin-hadoop2.7 /opt/spark-2.2.1}}

4. Setup bash profile for spark. 
{{vi ~/.bash_profile}}
Enter the below
{code}
export SPARK_HOME=/opt/spark-2.2.1
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
{code}
{{source .bash_profile}}


Run the command: {{spark-submit}}
Expected output {{Usage: spark-submit [options] ...}} plus many more lines. 