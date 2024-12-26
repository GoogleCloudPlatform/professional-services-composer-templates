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
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)



# Define variables
gcs_bucket_name = "sample-bq-test-2"
gcs_bucket_prefix = "gcs-sensor/"
bash_command = 'echo {{ ti.xcom_pull(task_ids="gcs_file_sensor") }}'


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
    dag_id='gcs_sensor_tasks_dag',
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
        
    gcs_file_sensor = GCSObjectsWithPrefixExistenceSensor(
        bucket = gcs_bucket_name,
        mode = "poke",
        prefix = gcs_bucket_prefix,
        task_id = "gcs_file_sensor",
        trigger_rule = "all_done",
    )
        
    print_file_names_from_gcs = BashOperator(
        bash_command = bash_command,
        task_id = "print_file_names_from_gcs",
        trigger_rule = "all_done",
    )
        
    gcs_delete_file = GCSDeleteObjectsOperator(
        bucket_name = gcs_bucket_name,
        prefix = gcs_bucket_prefix,
        task_id = "gcs_delete_file",
        trigger_rule = "all_done",
    )

    gcs_file_sensor >> print_file_names_from_gcs
    print_file_names_from_gcs >> gcs_delete_file