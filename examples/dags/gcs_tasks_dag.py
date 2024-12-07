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
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteBucketOperator


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
    dag_id='gcs_tasks_dag',
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
        
    create_bucket = GCSCreateBucketOperator(
        task_id = "create_bucket",
        project_id = "composer-templates-dev",
        bucket_name = "test-bucket12345654321",
        storage_class = "MULTI_REGIONAL",
        labels = {'env': 'dev', 'team': 'airflow'},
        trigger_rule = "none_failed",
    )
        
    synchronize_bucket = GCSSynchronizeBucketsOperator(
        task_id = "synchronize_bucket",
        source_bucket = "pub",
        source_object = "shakespeare/rose.txt",
        destination_bucket = "test-bucket12345654321",
        destination_object = "shakespeare/rose.txt",
        trigger_rule = "none_failed",
    )
        
    list_objects = GCSListObjectsOperator(
        task_id = "list_objects",
        bucket = "test-bucket12345654321",
        prefix = "shakespeare/",
        trigger_rule = "none_failed",
    )
        
    delete_bucket = GCSDeleteBucketOperator(
        task_id = "delete_bucket",
        bucket_name = "test-bucket12345654321",
        trigger_rule = "all_done",
    )


    create_bucket >> synchronize_bucket
    synchronize_bucket >> list_objects
    list_objects >> delete_bucket