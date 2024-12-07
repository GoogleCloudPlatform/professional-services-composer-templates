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
from google.cloud import spanner
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)




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
    dag_id='gcs_sensor_tasks_dag',
    default_args=default_args,
    schedule='None',
    description='None',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=6),
    tags=['test'],
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2024, 12, 1),
    max_active_tasks=None
)


with dag:
        
    gcs_file_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id = "gcs_file_sensor",
        bucket = "sample-bq-test-2",
        prefix = "gcs-sensor/",
        mode = "poke",
        trigger_rule = "all_done",
    )
        
    print_file_names_from_gcs = BashOperator(
        task_id = "print_file_names_from_gcs",
        bash_command = "echo {{ ti.xcom_pull(task_ids=\"gcs_file_sensor\") }}",
        trigger_rule = "all_done",
    )
        
    gcs_delete_file = GCSDeleteObjectsOperator(
        task_id = "gcs_delete_file",
        bucket_name = "sample-bq-test-2",
        prefix = "gcs-sensor/",
        trigger_rule = "all_done",
    )


    gcs_file_sensor >> print_file_names_from_gcs
    print_file_names_from_gcs >> gcs_delete_file