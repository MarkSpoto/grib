# Grib Spark app

Grib spark is a distributed Spark application engine that process grib files.

 
## Requirements
 * Linux
 * Java 8 64-bit. Both Oracle JDK and OpenJDK are supported.
 * Maven 3.3.9+ (for building)
 * Scala 2.11.12
 
## Building Grib Spark from command-line

Clone repo to server with connectivity to the target hadoop cluster

```bash
git clone git@bitbucket.org:phdata/grib_processing.git
```
 
Grib Spark is a standard Maven project. Simply run the following command from the project root directory:
    mvn -P valhalla clean install

Here you must specify a profile with the `-P` option. This will provide an environment specific build. 
Options can be `chs`.    

### Building Grib Spark in your IDE

Because Grib Spark is a standard Maven project, you can import it into your IDE using the root `pom.xml` file. 
We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). In IntelliJ, choose Open Project from the Quick 
Start box or choose Open from the File menu and select the root `pom.xml` file. 

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a 1.7 JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 7.0
only support Java 1.8.  

Grib Spark comes with two Profiles valhalla that should work out-of-the-box. Select one profile then 
in the Maven lifecycle click on the clean then install.

## Project structure
The project consist of two modules: 
* core java grib: the module is responsible for reading the grib file and get the needed data.  
* scala: It mainly submit the spark job to the yarn cluster. 

## Deployment Instructions
To deploy it to the cluster, unzip the tarball generated in the `/target`
directory and copy that directory to the target directory on the cluster.
```bash
tar xzf ./target/grib-{VERSION}-oozie-bundle-{ENVIRONMENT}.tar.gz
hdfs dfs -put -f ./target/GRIB-{ENVIRONMENT}
```


## Execution
Once the oozie application has been deployed, you can execute it as
shown below.
```bash
# If you don't already have this set, make sure to set it
# note that this is the valhalla server, update it to whatever
# it needs to be on your cluster.
export OOZIE_URL=http://master1.valhalla.phdata.io:11000/oozie/
oozie job -config ./target/GRIB-{ENVIRONMENT}/job.properties -run
```


### Documentation

Hence, because this project contains both Scala and Java code the documentation is 
per-individual module. After you compile the root project look for the generated 
docs in `~/target/site/scaladocs/index.html`


