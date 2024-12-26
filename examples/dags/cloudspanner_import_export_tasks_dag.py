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
from google.cloud import spanner, storage
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.spanner import SpannerQueryDatabaseInstanceOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)


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

def print_args(*args):
  """Prints all arguments passed to the function.

  Args:
    *args: Any number of arguments.
  """
  for arg in args:
    print(arg)

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



# Define variables
gcp_project_id = "composer-templates-dev"
spanner_instance_id = "composer-templates-spannerdb"
spanner_databse_id = "dev-spannerdb"
spanner_table = "Products"
spanner_table_columns = ["ProductId","ProductName","Description","Price","LastModified"]
spanner_sql_query = "DELETE FROM Products WHERE LastModified < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 DAY);"
import_gcs_bucket_name = "composer-templates-dev-input-files"
import_gcs_file_name = "spanner_input/sample_data_spanner.csv"
export_gcs_bucket_name = "hmh_backup"
export_gcs_file_name = "spanner_output/product_data_output.csv"
spanner_sql_export_query = "SELECT * FROM Products ;"

# Define Airflow DAG default_args
default_args = {
    "owner": 'test',
    "depends_on_past": False,
    "retries": 3,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ['test@example.com'],
    "retry_delay": timedelta(minutes=1),
    "mode": 'reschedule',
    "poke_interval": 120,
    "sla": timedelta(minutes=60),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=60)
}


dag = DAG(
    dag_id='cloudspanner_import_export_tasks_dag',
    default_args=default_args,
    schedule=None,
    description='Sample example to import/export data in and out of cloud spanner.',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=3),
    tags=['spanner', 'test', 'v1.1'],
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2024, 12, 1),max_active_tasks=5,
    
)


with dag:
        
    gcs_to_spanner = PythonOperator(
        op_kwargs = {
          "project_id": gcp_project_id,
          "instance_id": spanner_instance_id,
          "database_id": spanner_databse_id,
          "bucket_name": import_gcs_bucket_name,
          "file_name": import_gcs_file_name,
          "table_name": spanner_table,
          "columns": spanner_table_columns
        },
        python_callable = upload_gcs_to_spanner,
        task_id = "gcs_to_spanner",
        trigger_rule = "all_done",
    )
        
    delete_results_from_spanner = SpannerQueryDatabaseInstanceOperator(
        database_id = spanner_databse_id,
        instance_id = spanner_instance_id,
        project_id = gcp_project_id,
        query = spanner_sql_query,
        task_id = "delete_results_from_spanner",
        trigger_rule = "all_done",
    )
        
    spanner_to_gcs = PythonOperator(
        op_kwargs = {"project_id":gcp_project_id,"instance_id":spanner_instance_id,"database_id":spanner_databse_id,"bucket_name":export_gcs_bucket_name,"file_name":export_gcs_file_name,"sql_query":spanner_sql_export_query},
        python_callable = export_spanner_to_gcs,
        task_id = "spanner_to_gcs",
        trigger_rule = "all_done",
    )
        
    sample_python = PythonOperator(
        op_args = ['Composer', 'template', gcp_project_id],
        python_callable = print_args,
        task_id = "sample_python",
    )
        
    with TaskGroup(group_id="my_task_group1") as my_task_group1:
                
        bash_example1 = BashOperator(
            bash_command = "echo \"bash_example1\"\ndate\nls -l\n",
            task_id = "bash_example1",
        )
                
        bash_example2 = BashOperator(
            bash_command = "echo \"bash_example2\"\npwd\n",
            task_id = "bash_example2",
        )
        
    with TaskGroup(group_id="extract_data") as extract_data:
                
        extract_from_source_a = PostgresOperator(
            sql = "SELECT * FROM source_a;",
            task_id = "extract_from_source_a",
        )
                
        extract_from_source_b = PostgresOperator(
            sql = "SELECT * FROM source_b;",
            task_id = "extract_from_source_b",
        )

            
            
    my_task_group1 >> extract_data
    bash_example1 >> extract_from_source_a
    bash_example2 >> extract_from_source_b
    extract_data >> gcs_to_spanner
    gcs_to_spanner >> delete_results_from_spanner
    delete_results_from_spanner >> spanner_to_gcs
    delete_results_from_spanner >> sample_python