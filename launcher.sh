mvn clean package


spark-submit \
--class Log_Analysis \
--jars target/LogAnalysis-0.1.jar \
data/log10.txt 