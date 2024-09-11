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
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator

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
        dag_id='dataform_tasks_dag',
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

    create_compilation_result = DataformCreateCompilationResultOperator (
            task_id = 'create_compilation_result',
            project_id = 'composer-templates-dev',
            region = 'us-central1',
            repository_id = 'quickstart-production',
            compilation_result = {'git_commitish': 'main'},
            trigger_rule = 'none_failed',
        )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator (
            task_id = 'create_workflow_invocation',
            project_id = 'composer-templates-dev',
            region = 'us-central1',
            repository_id = 'quickstart-production',
            asynchronous = True,
            workflow_invocation = {'compilation_result': "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"},
            trigger_rule = 'none_failed',
        )
    
    start >> create_compilation_result >> create_workflow_invocation