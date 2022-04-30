# Log Ananlysis with Scala-Spark

Project work for Languages and Algorithms for Artificial Intelligence.

- [Abstract](##Abstract)
- [Technologies](##Tech)
- [Requisites](##Requisites)
- [How to run it](##Howtorunit)


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

## How to run it
There is a script called launcher.sh that will compile and run the main function.
```
./launcher.sh
```