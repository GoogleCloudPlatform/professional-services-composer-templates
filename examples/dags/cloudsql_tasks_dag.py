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
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExportInstanceOperator


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
    dag_id='cloudsql_tasks_dag',
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
        
    cloud_sql_truncate_sales_table_task = CloudSQLExecuteQueryOperator(
        task_id = "cloud_sql_truncate_sales_table_task",
        gcp_cloudsql_conn_id = "airflow_composer_template_mysql",
        sql = "TRUNCATE TABLE sales;",
        trigger_rule = "all_done",
    )
        
    cloud_sql_import_sales_data_from_gcs = CloudSQLImportInstanceOperator(
        task_id = "cloud_sql_import_sales_data_from_gcs",
        instance = "composer-template",
        body = {"importContext": {"csvImportOptions": {"escapeCharacter": "5C", "fieldsTerminatedBy": "2C", "linesTerminatedBy": "0A", "quoteCharacter": "22", "table": "sales"}, "database": "transactions", "fileType": "CSV", "uri": "gs://hmh_composer_demo/demo_test_csv/daily_sales.csv"}},
        trigger_rule = "all_done",
    )
        
    cloud_sql_drop_sales_reporting_table_task = CloudSQLExecuteQueryOperator(
        task_id = "cloud_sql_drop_sales_reporting_table_task",
        gcp_cloudsql_conn_id = "airflow_composer_template_mysql",
        sql = "DROP TABLE IF EXISTS sales_reporting;",
        trigger_rule = "all_done",
    )
        
    cloud_sql_create_sales_reporting_table_task = CloudSQLExecuteQueryOperator(
        task_id = "cloud_sql_create_sales_reporting_table_task",
        gcp_cloudsql_conn_id = "airflow_composer_template_mysql",
        sql = "create table sales_reporting as select s.sale_id, p.product_name, p.description, s.order_date, s.city, s.state, p.price as product_price, s.quantity, (s.quantity*p.price) as actual_sell_price, s.total_price as sell_price, s.total_price - (s.quantity*p.price) as difference from sales s inner join products p on s.product_id = p.product_id;",
        trigger_rule = "all_done",
    )
        
    export_sales_reporting_table_to_gcs = CloudSQLExportInstanceOperator(
        task_id = "export_sales_reporting_table_to_gcs",
        instance = "composer-template",
        body = {"exportContext": {"csvExportOptions": {"selectQuery": "SELECT * FROM sales_reporting;"}, "databases": ["transactions"], "fileType": "CSV", "offload": true, "uri": "gs://hmh_composer_demo/demo_test_csv/sales_report.csv"}},
        trigger_rule = "all_done",
    )


    cloud_sql_truncate_sales_table_task >> cloud_sql_import_sales_data_from_gcs
    cloud_sql_import_sales_data_from_gcs >> cloud_sql_drop_sales_reporting_table_task
    cloud_sql_drop_sales_reporting_table_task >> cloud_sql_create_sales_reporting_table_task
    cloud_sql_create_sales_reporting_table_task >> export_sales_reporting_table_to_gcs