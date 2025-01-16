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
from airflow.providers.google.cloud.operators.dlp import CloudDLPCreateDLPJobOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)



# Define variables
gcp_project_id = "composer-templates-dev"
dlp_job_payload = {"storage_config": {"cloud_storage_options": {"file_set": {"url": "gs://composer-templates-dev-input-files/dlp_operator_input/"}, "file_types": ["TEXT_FILE"]}}, "inspect_config": {"info_types": [{"name": "PHONE_NUMBER"}, {"name": "US_SOCIAL_SECURITY_NUMBER"}, {"name": "EMAIL_ADDRESS"}], "min_likelihood": "VERY_UNLIKELY"}, "inspect_template_name": "projects/composer-templates-dev/locations/global/inspectTemplates/demo-composer-template-inspect-test", "actions": [{"save_findings": {"output_config": {"table": {"project_id": f"{gcp_project_id}", "dataset_id": "test_dataset", "table_id": "dlp_googleapis_2024_11_02_5447016892596828032"}}}}, {"deidentify": {"transformation_config": {"deidentify_template": "projects/composer-templates-dev/locations/global/deidentifyTemplates/demo-composer-template-test"}, "cloud_storage_output": "gs://hmh_backup/deidentified-data_output/"}}]}


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
    dag_id='data_loss_preventive_deidentify_tasks_job',
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
        
    trigger_dlp_deindentify_job = CloudDLPCreateDLPJobOperator(
        inspect_job = dlp_job_payload,
        project_id = gcp_project_id,
        task_id = "trigger_dlp_deindentify_job",
        trigger_rule = "all_done",
    )
