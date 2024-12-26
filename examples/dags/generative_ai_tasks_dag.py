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
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import SupervisedFineTuningTrainOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import CountTokensOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import GenerativeModelGenerateContentOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)


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



# Define variables
project_id = "composer-templates-dev"
region = "us-central1"
pro_model = "gemini-1.5-pro-002"
system_instruction = "Always respond in plain-text only."
sample_prompt = ["Summarize the following article. Article: To make a classic spaghetti carbonara, start by bringing a large pot of salted water to a boil. While the water is heating up, cook pancetta or guanciale in a skillet with olive oil over medium heat until it's crispy and golden brown. Once the pancetta is done, remove it from the skillet and set it aside. In the same skillet, whisk together eggs, grated Parmesan cheese, and black pepper to make the sauce. When the pasta is cooked al dente, drain it and immediately toss it in the skillet with the egg mixture, adding a splash of the pasta cooking water to create a creamy sauce."]
deterministic_gen_config = { 
    "top_k": 1,
    "top_p": 0.0,
    "temperature": 0.1
}

validate_tokens_op_kwargs = {
    "character_budget":"1000",
    "token_budget":"500",
    "total_billable_characters":"{{ task_instance.xcom_pull(task_ids='count_tokens_task', key='total_billable_characters') }}",
    "total_tokens":"{{ task_instance.xcom_pull(task_ids='count_tokens_task', key='total_tokens') }}"
}


# Define Airflow DAG default_args
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
    schedule="@once",
    description='Demonstration of Vertex Generative AI ops on Airflow/Composer',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=6),
    tags=['demo', 'vertex_ai', 'generative_ai'],
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2024, 12, 1),
    
)


with dag:
        
    training_data_exists_sensor = GCSObjectExistenceSensor(
        bucket = "cloud-samples-data",
        object = "ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl",
        task_id = "training_data_exists_sensor",
        trigger_rule = "all_success",
    )
        
    sft_train_base_task = SupervisedFineTuningTrainOperator(
        location = region,
        project_id = project_id,
        source_model = pro_model,
        task_id = "sft_train_base_task",
        train_dataset = "gs://cloud-samples-data/ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl",
        trigger_rule = "all_success",
    )
        
    count_tokens_task = CountTokensOperator(
        contents = sample_prompt,
        location = region,
        pretrained_model = "{{ task_instance.xcom_pull(task_ids=\"sft_train_base_task\", key=\"tuned_model_endpoint_name\") }}",
        project_id = project_id,
        task_id = "count_tokens_task",
        trigger_rule = "all_success",
    )
        
    validate_tokens_task = PythonOperator(
        op_kwargs = validate_tokens_op_kwargs,
        provide_context = True,
        python_callable = validate_tokens,
        task_id = "validate_tokens_task",
        trigger_rule = "all_success",
    )
        
    deterministic_generate_content_task = GenerativeModelGenerateContentOperator(
        contents = sample_prompt,
        generation_config = deterministic_gen_config,
        location = region,
        pretrained_model = "{{ task_instance.xcom_pull(task_ids=\"sft_train_base_task\", key=\"tuned_model_endpoint_name\") }}",
        project_id = project_id,
        system_instruction = system_instruction,
        task_id = "deterministic_generate_content_task",
        trigger_rule = "all_success",
    )

    training_data_exists_sensor >> sft_train_base_task
    sft_train_base_task >> count_tokens_task
    count_tokens_task >> validate_tokens_task
    validate_tokens_task >> deterministic_generate_content_task