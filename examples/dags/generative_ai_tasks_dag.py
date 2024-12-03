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
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import SupervisedFineTuningTrainOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import CountTokensOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import GenerativeModelGenerateContentOperator

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
    source_blob_name="dag_variables/generative_ai_tasks_variables.yaml"
)

if type(run_time_config_data["dev"]) is dict:
    env_configs = run_time_config_data["dev"]

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

# Sample example of a python functions to import from the file to add to airflow DAG

def validate_tokens(character_budget, token_budget, total_billable_characters, total_tokens):
    return int(total_billable_characters) < int(character_budget) and int(total_tokens) < int(token_budget)

default_args = {
    "owner": 'Google',
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=55),
    "execution_timeout": timedelta(minutes=60),
    "start_date": '2024-01-01',
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=55),
    "execution_timeout": timedelta(minutes=60)
}

dag = DAG(
    dag_id='generative_ai_tasks_dag',
    default_args=default_args,
    schedule='@once',
    description='Demonstration of Vertex Generative AI ops on Airflow/Composer',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=6),
    tags=['demo', 'vertex_ai', 'generative_ai'],
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2024, 12, 1),
    max_active_tasks=None,
    is_paused_upon_creation=True
)

with dag:

    start = DummyOperator(task_id='start')

    training_data_exists_sensor = GCSObjectExistenceSensor (
            task_id = 'training_data_exists_sensor',
            bucket = 'cloud-samples-data',
            object = 'ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl',
            trigger_rule = 'all_success',
        )

    sft_train_base_task = SupervisedFineTuningTrainOperator (
            task_id = 'sft_train_base_task',
            project_id = env_configs.get('cc_var_project_id'),
            location = env_configs.get('cc_var_region'),
            source_model = env_configs.get('cc_var_pro_model'),
            train_dataset = 'gs://cloud-samples-data/ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl',
            trigger_rule = 'all_success',
        )

    count_tokens_task = CountTokensOperator (
            task_id = 'count_tokens_task',
            project_id = env_configs.get('cc_var_project_id'),
            location = env_configs.get('cc_var_region'),
            pretrained_model = '{{ task_instance.xcom_pull(task_ids="sft_train_base_task", key="tuned_model_endpoint_name") }}',
            contents = env_configs.get('cc_var_sample_prompt'),
            trigger_rule = 'all_success',
        )

    validate_tokens_task = PythonOperator (
            task_id = 'validate_tokens_task',
            python_callable = validate_tokens,
            op_kwargs ={
            'character_budget' : '1000',
            'token_budget' : '500',
            'total_billable_characters' : '{{ task_instance.xcom_pull(task_ids="count_tokens_task", key="total_billable_characters") }}',
            'total_tokens' : '{{ task_instance.xcom_pull(task_ids="count_tokens_task", key="total_tokens") }}',
            },
            provide_context = True,
            trigger_rule = 'all_success',
        )

    deterministic_generate_content_task = GenerativeModelGenerateContentOperator (
            task_id = 'deterministic_generate_content_task',
            project_id = env_configs.get('cc_var_project_id'),
            location = env_configs.get('cc_var_region'),
            pretrained_model = '{{ task_instance.xcom_pull(task_ids="sft_train_base_task", key="tuned_model_endpoint_name") }}',
            system_instruction = env_configs.get('cc_var_system_instruction'),
            contents = env_configs.get('cc_var_sample_prompt'),
            generation_config = env_configs.get('cc_var_deterministic_gen_config'),
            tools = [],
            trigger_rule = 'all_success',
        )
    
    start >> training_data_exists_sensor >> sft_train_base_task >> count_tokens_task >> validate_tokens_task >> deterministic_generate_content_task