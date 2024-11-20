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
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.spanner import SpannerQueryDatabaseInstanceOperator

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
    source_blob_name="dag_variables/composer_variables_test.yaml"
)

if type(run_time_config_data["dev"]) is dict:
    env_configs = run_time_config_data["dev"]


def upload_gcs_to_spanner(
  project_id:str, instance_id:str, database_id:str, bucket_name:str, file_name:str, table_name:str,columns:list):
  """Uploads data from a CSV file in GCS to a Cloud Spanner table."""
  """
  :param str project_id: Google Cloud Project ID
  :param str instance_id: Google Cloud Spanner Instance ID
  :param str database_id: Google Cloud Spanner Database ID
  :param str bucket_name: Google Cloud Storage Bucket to read files
  :param str file_name: Filename to import to 
  :param str table_name: Cloud Spanner Table name for importing data
  :param list columns: Cloud Spanner table column names in ascending order as per DDL (assumed that input file is in same order)
  :return dict : GCS file path and Spanner details
  """

  # Initialize Cloud Spanner client
  spanner_client = spanner.Client(project=project_id)
  instance = spanner_client.instance(instance_id)
  database = instance.database(database_id)

  # Initialize Cloud Storage client
  storage_client = storage.Client(project=project_id)
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(file_name)

  # Download the file from GCS
  data = blob.download_as_string()

  # Parse the CSV data
  rows = []
  for line in data.decode("utf-8").splitlines():
      row = line.split(",")
      rows.append(row)

  # Insert data into Cloud Spanner table
  cols = columns
  with database.batch() as batch:
      batch.insert(table=table_name, columns=cols, values=rows)

  print(f"Data from {file_name} uploaded to {table_name} successfully.")
  return {"input_file_path":"gs://"+f"{bucket_name}"+"/"+f"{file_name}", "spanner_databse_table":f"{database_id}"+":"+f"{table_name}"}



def export_spanner_to_gcs(
  project_id, instance_id, database_id, bucket_name, file_name, sql_query):
  """Exports data from Cloud Spanner to a CSV file in GCS."""
  """
  :param str project_id: Google Cloud Project ID
  :param str instance_id: Google Cloud Spanner Instance ID
  :param str database_id: Google Cloud Spanner Database ID
  :param str bucket_name: Google Cloud Storage Bucket to export files to
  :param str sql_query: Spanner SQL query to fetch data from spanner 
  :return dict : GCS output file path
  """

  # Initialize Cloud Spanner client
  spanner_client = spanner.Client(project=project_id)
  instance = spanner_client.instance(instance_id)
  database = instance.database(database_id)

  # Initialize Cloud Storage client
  storage_client = storage.Client(project=project_id)
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(file_name)

  # Execute the query and fetch data
  with database.snapshot() as snapshot:
      results = snapshot.execute_sql(sql_query)

  # Format data as CSV
  csv_data = ""
  for row in results:
      csv_data += ",".join(str(value) for value in row) + "\n"

  # Upload the CSV data to GCS
  blob.upload_from_string(csv_data)

  print(f"Data exported to {file_name} in GCS successfully.")
  return {"output_file_path":"gs://"+f"{bucket_name}"+"/"+f"{file_name}"}


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
        dag_id='cloudspanner_import_export_tasks_dag',
        default_args = default_args,
        schedule_interval=None,
        description='Sample example to import/export data in and out of cloud spanner.',
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=True,
        tags=['spanner', 'test', 'v1.1'],
        start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    start = DummyOperator(task_id='start')

    gcs_to_spanner = PythonOperator (
            task_id = 'gcs_to_spanner',
            python_callable = upload_gcs_to_spanner,
            op_kwargs ={
            'project_id': env_configs.get('cc_var_gcp_project_id'),
            'instance_id': env_configs.get('cc_var_spanner_instance_id'),
            'database_id': env_configs.get('cc_var_spanner_databse_id'),
            'bucket_name': env_configs.get('cc_var_import_gcs_bucket_name'),
            'file_name': env_configs.get('cc_var_import_gcs_file_name'),
            'table_name': env_configs.get('cc_var_spanner_table'),
            'columns': env_configs.get('cc_var_spanner_table_columns'),
            },
            trigger_rule = 'all_done',
        )

    delete_results_from_spanner = SpannerQueryDatabaseInstanceOperator (
            task_id = 'delete_results_from_spanner',
            instance_id = env_configs.get('cc_var_spanner_instance_id'),
            database_id = env_configs.get('cc_var_spanner_databse_id'),
            query = env_configs.get('cc_var_spanner_sql_query'),
            project_id = env_configs.get('cc_var_gcp_project_id'),
            trigger_rule = 'all_done',
        )

    spanner_to_gcs = PythonOperator (
            task_id = 'spanner_to_gcs',
            python_callable = export_spanner_to_gcs,
            op_kwargs ={
            'project_id': env_configs.get('cc_var_gcp_project_id'),
            'instance_id': env_configs.get('cc_var_spanner_instance_id'),
            'database_id': env_configs.get('cc_var_spanner_databse_id'),
            'bucket_name': env_configs.get('cc_var_export_gcs_bucket_name'),
            'file_name': env_configs.get('cc_var_export_gcs_file_name'),
            'sql_query': env_configs.get('cc_var_spanner_sql_export_query'),
            },
            trigger_rule = 'all_done',
        )
    
    
    start >> gcs_to_spanner >> delete_results_from_spanner >> spanner_to_gcs
    
    [start >> gcs_to_spanner] >> spanner_to_gcs
    
    start >> [delete_results_from_spanner,spanner_to_gcs]