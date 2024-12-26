# type: ignore

# Define variables
project_id = "composer-templates-dev"
region = "us-central1"
job_name = "health-check-dataflow-job-template"
parameters = {"inputFile": "gs://pub/shakespeare/rose.txt","output": "gs://us-central1-composer-templa-1ae2c9bc-bucket/data/dataflow_output"}
environment = {"ipConfiguration": "WORKER_IP_PRIVATE"}
flex_body = {"launchParameter":{"jobName":"test-flex-template","parameters":{"schema":"gs://us-central1-composer-templa-1ae2c9bc-bucket/data/schema/my-schema.avsc","outputFileFormat":"avro","outputBucket":"gs://us-central1-composer-templa-1ae2c9bc-bucket/data/flexoutput/","inputFileFormat":"csv","inputFileSpec":"gs://cloud-samples-data/bigquery/us-states/*.csv"},"environment":{"ipConfiguration":"WORKER_IP_PRIVATE"},"containerSpecGcsPath":"gs://dataflow-templates/latest/flex/File_Format_Conversion"}}
java_beam_jar = "gs://us-central1-composer-templa-1ae2c9bc-bucket/dataflow-wordcount-jar/word-count-beam-0.1.jar"
java_beam_pipeline = {"output": "gs://us-central1-composer-templa-1ae2c9bc-bucket/data/javadataflowout/","tempLocation":"gs://us-central1-composer-templa-1ae2c9bc-bucket/data/temp","stagingLocation":"gs://us-central1-composer-templa-1ae2c9bc-bucket/data/staging","usePublicIps": "False" }


# Define Airflow DAG default_args
default_args = {
    "owner": 'test',
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=55),
    "execution_timeout": timedelta(minutes=60)
}

