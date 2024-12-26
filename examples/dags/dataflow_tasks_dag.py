# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStopJobOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)



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




dag = DAG(
    dag_id='dataflow_tasks_dag',
    default_args=default_args,
    schedule=None,
    description='None',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=6),
    tags=['test'],
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2024, 12, 1),
    
)


with dag:
        
    start_templated_job = DataflowTemplatedJobStartOperator(
        environment = environment,
        job_name = job_name,
        location = region,
        parameters = parameters,
        project_id = project_id,
        task_id = "start_templated_job",
        template = "gs://dataflow-templates/latest/Word_Count",
        trigger_rule = "none_failed",
    )
        
    start_flex_templated_job = DataflowStartFlexTemplateOperator(
        body = flex_body,
        location = region,
        task_id = "start_flex_templated_job",
        trigger_rule = "none_failed",
        wait_until_finished = True,
    )
        
    start_dataflow_job = BeamRunJavaPipelineOperator(
        jar = java_beam_jar,
        job_class = "org.apache.beam.examples.WordCount",
        pipeline_options = java_beam_pipeline,
        runner = "DataflowRunner",
        task_id = "start_dataflow_job",
        trigger_rule = "none_failed",
    )
        
    stop_templated_job = DataflowStopJobOperator(
        job_name_prefix = job_name,
        location = region,
        task_id = "stop_templated_job",
        trigger_rule = "none_failed",
    )

    start_templated_job >> start_flex_templated_job
    start_templated_job >> start_dataflow_job
    start_flex_templated_job >> stop_templated_job