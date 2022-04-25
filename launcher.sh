mvn clean package


spark-submit \
--class LogAnalysis \
target/LogAnalysis-*.*.jar \
data/log10.txt