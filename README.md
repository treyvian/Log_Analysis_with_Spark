# Log Ananlysis with Scala-Spark

Project work for Languages and Algorithms for Artificial Intelligence.

- [Abstract](##Abstract)
- [Technologies](##Tech)
- [Requisites](##Requisites)
- [Compile](##Compile)
- [Run It](##RunIt)


## Abstract
It's a Web Server Log Analysis with Spark(Scala). 
The idea is to provide some functions to extract information from the txt file in input containing unparsed data(logs). The data used for testing comes from a [zip](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/3QBYB5) found online. A log entry in the dataframe is parsed based on the [Common Log Format](https://en.wikipedia.org/wiki/Common_Log_Format).

## Tech
This project is built using:
- [Scala](https://www.scala-lang.org/) - programming language which supports both object-oriented programming and functional programming.
- [Apache Maven](https://maven.apache.org/) - software project management and comprehension tool
- [Apache-Spark](https://spark.apache.org/) - an open-source unified analytics engine for large-scale data processing.

## Requisites
* java-11-openjdk or java-8-openjdk (see Spark spcification)
* Scala v2.12
* Apache-Spark v3.2.0

## Compile
The code is compiled using Maven. To run a clean compile the code run in the project directory:
```
mvn clean compile package
```

## Run it
To run it locally the command is:
```
spark-submit --class LogAnalysis target/LogAnalysis-*.*.jar local <INPUT_FOLDER> <OUTPUT_FOLDER>
```
INPUT_FOLDER = the file of log in input, for this project: data/access.log
OUTPUT_FOLDER = the folder in which to store the output: output/

### Run it with Google Cloud Platform
First upload the file to Google Storage
```
gsutil cp target/LogAnalysis-0.1.jar \
   gs://${BUCKET_NAME}/scala/LogAnalysis-0.1.jar
```
Then you can run it with:
```
gcloud dataproc jobs submit spark \
  --cluster=${CLUSTER} \
  --class=LogAnalysis \
  --jars=gs://${BUCKET_NAME}/scala/LogAnalysis-0.1.jar \
  --region=${REGION} \
  --gcp gs://${BUCKET_NAME}/input/access.log gs://${BUCKET_NAME}/output/
```

