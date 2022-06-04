# Compile the project with Maven
mvn clean package

# Launch the main function 
spark-submit \
--class LogAnalysis \
target/LogAnalysis-*.*.jar \
data/access.log output/


###gcloud dataproc jobs submit spark \
#    --cluster=${CLUSTER} \
#    --class=LogAnalysis \
#    --jars=gs://${BUCKET_NAME}/scala/LogAnalysis-0.1.jar \
#    --region=${REGION} \
#    -- gs://${BUCKET_NAME}/input/access.log gs://${BUCKET_NAME}/output/


# gsutil cp target/LogAnalysis-0.1.jar \
#    gs://${BUCKET_NAME}/scala/LogAnalysis-0.1.jar
