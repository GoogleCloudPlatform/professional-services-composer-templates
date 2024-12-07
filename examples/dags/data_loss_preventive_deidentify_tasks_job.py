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
from airflow.providers.google.cloud.operators.dlp import CloudDLPCreateDLPJobOperator


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
    dag_id='data_loss_preventive_deidentify_tasks_job',
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
        
    trigger_dlp_deindentify_job = CloudDLPCreateDLPJobOperator(
        task_id = "trigger_dlp_deindentify_job",
        project_id = "composer-templates-dev",
        inspect_job = {"actions": [{"save_findings": {"output_config": {"table": {"dataset_id": "test_dataset", "project_id": "composer-templates-dev", "table_id": "dlp_googleapis_2024_11_02_5447016892596828032"}}}}, {"deidentify": {"cloud_storage_output": "gs://hmh_backup/deidentified-data_output/", "transformation_config": {"deidentify_template": "projects/composer-templates-dev/locations/global/deidentifyTemplates/demo-composer-template-test"}}}], "inspect_config": {"info_types": [{"name": "PHONE_NUMBER"}, {"name": "US_SOCIAL_SECURITY_NUMBER"}, {"name": "EMAIL_ADDRESS"}], "min_likelihood": "VERY_UNLIKELY"}, "inspect_template_name": "projects/composer-templates-dev/locations/global/inspectTemplates/demo-composer-template-inspect-test", "storage_config": {"cloud_storage_options": {"file_set": {"url": "gs://composer-templates-dev-input-files/dlp_operator_input/"}, "file_types": ["TEXT_FILE"]}}},
        trigger_rule = "all_done",
    )

