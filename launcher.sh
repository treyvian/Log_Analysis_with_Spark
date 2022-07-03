echo "COMPILING PROJECT"

# Compile the project with Maven
mvn clean package

export CLUSTER=cluster1
export BUCKET_NAME=log-analysis-data
export REGION=europe-west6

echo "LOADING JAR TO THE CLUSTER"

# Copying jar to cloud
gsutil cp target/LogAnalysis-1.0.jar \
   gs://${BUCKET_NAME}/scala/LogAnalysis-1.0.jar

echo "EXECUTING JOB"

# Excecution of the job on cloud
gcloud dataproc jobs submit spark \
   --cluster=${CLUSTER} \
   --class=LogAnalysis \
   --jars=gs://${BUCKET_NAME}/scala/LogAnalysis-1.0.jar \
   --region=${REGION} \
   -- yarn gs://${BUCKET_NAME}/input/access.log gs://${BUCKET_NAME}/output/



