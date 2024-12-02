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

def validate_tokens(character_budget, token_budget, total_billable_characters, total_tokens):
    return int(total_billable_characters) < int(character_budget) and int(total_tokens) < int(token_budget)


default_args = {
        "owner": 'Google',
        "retries": 0,
        "email_on_failure": False,
        "email_on_retry": False,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=55),
        "execution_timeout": timedelta(minutes=60),
        "depends_on_past": False,
}

dag = DAG(
        dag_id='generative_ai_tasks_dag',
        default_args = default_args,
        schedule_interval='@once',
        description='Demonstration of Vertex Generative AI ops on Airflow/Composer',
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=True,
        tags=['demo', 'vertex_ai', 'generative_ai'],
        start_date=airflow.utils.dates.days_ago(0)
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
            project_id = "cy-artifacts",
            location = "us-central1",
            source_model = "gemini-1.5-pro-002",
            train_dataset = 'gs://cloud-samples-data/ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl',
            trigger_rule = 'all_success',
        )

    count_tokens_task = CountTokensOperator (
            task_id = 'count_tokens_task',
            project_id = "cy-artifacts",
            location = "us-central1",
            pretrained_model = '{{ task_instance.xcom_pull(task_ids="sft_train_base_task", key="tuned_model_endpoint_name") }}',
            contents = ["Summarize the following article. Article: To make a classic spaghetti carbonara, start by bringing a large pot of salted water to a boil. While the water is heating up, cook pancetta or guanciale in a skillet with olive oil over medium heat until it\u0027s crispy and golden brown. Once the pancetta is done, remove it from the skillet and set it aside. In the same skillet, whisk together eggs, grated Parmesan cheese, and black pepper to make the sauce. When the pasta is cooked al dente, drain it and immediately toss it in the skillet with the egg mixture, adding a splash of the pasta cooking water to create a creamy sauce."],
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
            project_id = "composer-templates-dev",
            location = "us-central1",
            pretrained_model = '{{ task_instance.xcom_pull(task_ids="sft_train_base_task", key="tuned_model_endpoint_name") }}',
            system_instruction = "Always respond in plain-text only.",
            contents = ["Summarize the following article. Article: To make a classic spaghetti carbonara, start by bringing a large pot of salted water to a boil. While the water is heating up, cook pancetta or guanciale in a skillet with olive oil over medium heat until it\u0027s crispy and golden brown. Once the pancetta is done, remove it from the skillet and set it aside. In the same skillet, whisk together eggs, grated Parmesan cheese, and black pepper to make the sauce. When the pasta is cooked al dente, drain it and immediately toss it in the skillet with the egg mixture, adding a splash of the pasta cooking water to create a creamy sauce."],
            generation_config = {"temperature": 0.1, "top_k": 1, "top_p": 0.0},
            tools = [],
            trigger_rule = 'all_success',
        )
    
    start >> training_data_exists_sensor >> sft_train_base_task >> count_tokens_task >> validate_tokens_task >> deterministic_generate_content_task