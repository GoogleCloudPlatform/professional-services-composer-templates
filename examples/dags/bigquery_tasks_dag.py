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
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)




def transformation(data):
  """
  Sample function as an example to perform custom transformation.

  Args:
    data: Sample data on which we can perform any transformation.
  
  Returns:
    The data converted into a string format.
  """
  print("Printing sample payload from transformation function: {}".format(data))
  output = str(data)
  return output

def pull_xcom(**kwargs):
  """
  Pulls a value from XCom and prints it.
  """
  ti = kwargs['ti']
  pulled_value = str(ti.xcom_pull(task_ids='export_sales_reporting_table_to_gcs', key='file_details'))
  print(f"Pulled value from XCom: {pulled_value}")
  return pulled_value


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
    dag_id='bigquery_tasks_dag',
    default_args=default_args,
    schedule='@hourly',
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
        
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = "create_bq_dataset",
        dataset_id = "test_dataset",
        project_id = "composer-templates-dev",
        trigger_rule = "none_failed",
    )
        
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id = "create_bq_table",
        dataset_id = "test_dataset",
        table_id = "test_table",
        schema_fields = [{'name': 'emp_name', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'salary', 'type': 'INTEGER', 'mode': 'NULLABLE'}],
        trigger_rule = "none_failed",
    )
        
    get_data_from_bq_table = BigQueryGetDataOperator(
        task_id = "get_data_from_bq_table",
        dataset_id = "test_dataset",
        table_id = "test_table",
        trigger_rule = "none_failed",
    )
        
    export_to_gcs = BigQueryToGCSOperator(
        task_id = "export_to_gcs",
        source_project_dataset_table = "composer-templates-dev.hmh_demo.tmp_covid",
        destination_cloud_storage_uris = "gs://hmh_composer_demo/export_files/covid.csv",
        export_format = "csv",
        field_delimiter = ",",
        print_header = True,
        trigger_rule = "none_failed",
    )
        
    bq_query_execute = BigQueryExecuteQueryOperator(
        task_id = "bq_query_execute",
        use_legacy_sql = False,
        write_disposition = "WRITE_TRUNCATE",
        allow_large_results = True,
        destination_dataset_table = "composer-templates-dev.hmh_demo.tmp_covid",
        sql = "SELECT * FROM `composer-templates-dev.hmh_demo.covid` WHERE case_reported_date = \"2021-08-18\"",
        trigger_rule = "none_failed",
    )


    create_bq_dataset >> create_bq_table
    create_bq_dataset >> get_data_from_bq_table
    get_data_from_bq_table >> export_to_gcs
    export_to_gcs >> bq_query_execute