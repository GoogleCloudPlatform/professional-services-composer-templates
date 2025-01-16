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
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)


# Placeholder function for data processing
def process_data(ti, **kwargs):
  task_number = kwargs['task_number']
  ti.xcom_push(key=f'data_output_{task_number}', value=f'Processed data for task {task_number}')

# Placeholder function to process data outputs
def process_data_outputs(ti):
  # Example: Collect data outputs from dynamically generated tasks
  outputs = []
  for i in range(3):  # Adjust the range based on the number of dynamic tasks
    output = ti.xcom_pull(task_ids=f'process_data_{i}', key=f'data_output_{i}')
    if output:
      outputs.append(output)

  # Process the collected outputs
  print(f"Collected data outputs: {outputs}")



# Define variables
PROJECT_ID = 'your-gcp-project-id'  # Replace with your actual project ID
DATAPROC_CLUSTER = 'your-dataproc-cluster'  # Replace with your Dataproc cluster name
GCS_BUCKET = 'your-gcs-bucket'  # Replace with your GCS bucket name
REGION = 'your-gcp-region'  # Replace with your GCP region

# Define Airflow DAG default_args
default_args = {
    "depends_on_past": False,
    "email": 'test@example.com',
    "email_on_failure": False,
    "email_on_retry": False,
    "owner": 'test',
    "start_date": datetime(2024, 12, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=2), 
    "sla": timedelta(minutes=55),
    "execution_timeout": timedelta(minutes=60)
}


dag = DAG(
    dag_id='taskgroup_with_loops_and_conditions',
    default_args=default_args,
    schedule=None,
    description='TaskGroup example having loops and conditional statements',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=6),
    tags=['test'],
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2024, 12, 1),
    
)


with dag:
        
    get_config = PythonOperator(
        python_callable = lambda: {'environment': 'production', 'num_production_tasks': 3},
        task_id = "get_config",
    )
        
    process_outputs = PythonOperator(
        python_callable = process_data_outputs,
        task_id = "process_outputs",
        trigger_rule = "all_success",
    )
        
    finish = BigQueryExecuteQueryOperator(
        sql = "SELECT 'DAG completed!' AS status;",
        task_id = "finish",
        trigger_rule = "all_success",
        use_legacy_sql = False,
    )
        
    with TaskGroup(group_id="production_tasks") as production_tasks:
                
        # Use a for loop to create multiple tasks dynamically
        for i in range(3):  # You can use a variable here if needed
            f'process_data_{i}' = PythonOperator(
                op_kwargs = {'task_number': f'{i}'},
                python_callable = process_data,
                task_id = f'process_data_{i}',
            )
                
        finalize_production = BigQueryExecuteQueryOperator(
            sql = "SELECT CURRENT_TIMESTAMP() AS completion_time;",
            task_id = "finalize_production",
            use_legacy_sql = False,
        )
        
    with TaskGroup(group_id="staging_tasks") as staging_tasks:
                
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "spark_job": {
                "main_class": "your.spark.main.class",  # Replace with your Spark main class
                "jar_file_uris": [f"gs://{GCS_BUCKET}/your-spark-job.jar"],  # Use GCS_BUCKET variable
            }
        }
        run_spark_job = DataprocSubmitJobOperator(
            job = job,
            project_id = PROJECT_ID,
            region = REGION,
            task_id = "run_spark_job",
        )
                
        body={
            "transfer_job": {
              "description": "Transfer data to GCS",
              "project_id": PROJECT_ID,
              "transfer_spec": {
                "gcs_data_sink": {"bucket_name": GCS_BUCKET},
                # ... (add your transfer source configuration here) ...
              },
              "status": "ENABLED",
            }
        }
        transfer_data_to_gcs = CloudDataTransferServiceCreateJobOperator(
            body = body,
            task_id = "transfer_data_to_gcs",
        )

            
    get_config >> production_tasks
            
    get_config >> staging_tasks
    production_tasks >> process_outputs
    staging_tasks >> process_outputs
    process_outputs >> finish