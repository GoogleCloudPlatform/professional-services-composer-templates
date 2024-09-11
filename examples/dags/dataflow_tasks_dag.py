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


import os
import airflow
import yaml
import logging
from datetime import datetime, timedelta
from airflow.models import DAG
from typing import Any
from typing import Dict
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStopJobOperator

log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

default_args = {
        "owner": 'test',
        "retries": 1,
        "email_on_failure": False,
        "email_on_retry": False,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=55),
        "execution_timeout": timedelta(minutes=60),
}

dag = DAG(
        dag_id='dataflow_tasks_dag',
        default_args = default_args,
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=True,
        tags=['test'],
        start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    start = DummyOperator(task_id='start')

    start_templated_job = DataflowTemplatedJobStartOperator (
            task_id = 'start_templated_job',
            job_name = "health-check-dataflow-job-template",
            project_id = "composer-templates-dev",
            template = 'gs://dataflow-templates/latest/Word_Count',
            parameters = {"inputFile": "gs://pub/shakespeare/rose.txt", "output": "gs://us-central1-composer-templa-1ae2c9bc-bucket/data/dataflow_output"},
            environment = {"ipConfiguration": "WORKER_IP_PRIVATE"},
            location = "us-central1",
            trigger_rule = 'none_failed',
        )

    start_flex_templated_job = DataflowStartFlexTemplateOperator (
            task_id = 'start_flex_templated_job',
            location = "us-central1",
            body = {"launchParameter": {"containerSpecGcsPath": "gs://dataflow-templates/latest/flex/File_Format_Conversion", "environment": {"ipConfiguration": "WORKER_IP_PRIVATE"}, "jobName": "test-flex-template", "parameters": {"inputFileFormat": "csv", "inputFileSpec": "gs://cloud-samples-data/bigquery/us-states/*.csv", "outputBucket": "gs://us-central1-composer-templa-1ae2c9bc-bucket/data/flexoutput/", "outputFileFormat": "avro", "schema": "gs://us-central1-composer-templa-1ae2c9bc-bucket/data/schema/my-schema.avsc"}}},
            wait_until_finished = True,
            trigger_rule = 'none_failed',
        )

    start_dataflow_job = BeamRunJavaPipelineOperator (
            task_id = 'start_dataflow_job',
            runner = 'DataflowRunner',
            job_class = 'org.apache.beam.examples.WordCount',
            jar = "gs://us-central1-composer-templa-1ae2c9bc-bucket/dataflow-wordcount-jar/word-count-beam-0.1.jar",
            pipeline_options = {"output": "gs://us-central1-composer-templa-1ae2c9bc-bucket/data/javadataflowout/", "stagingLocation": "gs://us-central1-composer-templa-1ae2c9bc-bucket/data/staging", "tempLocation": "gs://us-central1-composer-templa-1ae2c9bc-bucket/data/temp", "usePublicIps": "False"},
            dataflow_config = {'check_if_running': 'CheckJobRunning.IgnoreJob', 'location': 'us-central1', 'poll_sleep': 10, 'job_name':'start_dataflow_job'},
            trigger_rule = 'none_failed',
        )

    stop_templated_job = DataflowStopJobOperator (
            task_id = 'stop_templated_job',
            location = "us-central1",
            job_name_prefix = "health-check-dataflow-job-template",
            trigger_rule = 'none_failed',
        )
    
    start >> start_templated_job >> [start_flex_templated_job,start_dataflow_job] >> stop_templated_job
