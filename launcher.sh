# Compile the project with Maven
mvn clean package

# Launch the main function 
spark-submit \
--class LogAnalysis \
target/LogAnalysis-*.*.jar \
data/log10.txt