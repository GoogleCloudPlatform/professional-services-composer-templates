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
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

log = logging.getLogger("airflow")
log.setLevel(logging.INFO)
composer_env_name = os.environ["COMPOSER_ENVIRONMENT"]
composer_env_bucket = os.environ["GCS_BUCKET"]
env_configs = {}

def load_config_from_gcs(bucket_name: str, source_blob_name: str) -> Dict[str, Any]:
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename("config.yaml")
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    return config

run_time_config_data = load_config_from_gcs(
    bucket_name=composer_env_bucket,
    source_blob_name="dag_configs/gcs_sensor_tasks_config.yaml"
)

for env, configs in run_time_config_data['envs'].items():
    if env == composer_env_name and type(configs) is dict:
        env_configs = configs

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
        dag_id='gcs_sensor_tasks_dag',
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

    gcs_file_sensor = GCSObjectsWithPrefixExistenceSensor (
            task_id = 'gcs_file_sensor',
            bucket = env_configs.get('cc_var_gcs_bucket_name'),
            prefix = env_configs.get('cc_var_gcs_bucket_prefix'),
            mode = 'poke',
            trigger_rule = 'all_done',
        )

    print_file_names_from_gcs = BashOperator (
            task_id = 'print_file_names_from_gcs',
            bash_command = env_configs.get('cc_var_bash_command'),
            trigger_rule = 'all_done',
        )

    gcs_delete_file = GCSDeleteObjectsOperator (
            task_id = 'gcs_delete_file',
            bucket_name = env_configs.get('cc_var_gcs_bucket_name'),
            prefix = env_configs.get('cc_var_gcs_bucket_prefix'),
            trigger_rule = 'all_done',
        )
    
    start >> gcs_file_sensor >> print_file_names_from_gcs >> gcs_delete_file