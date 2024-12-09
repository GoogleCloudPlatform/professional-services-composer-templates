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
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.operators.dummy import DummyOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)




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
    dag_id='nested_task_groups_dag',
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
    max_active_tasks=None
)


with dag:
        
    create_datafusion_pipeline = CloudDataFusionStartPipelineOperator(
        task_id = "create_datafusion_pipeline",
        pipeline_name = "test",
        instance_name = "test1",
        location = "us-west1",
        project_id = "composer-templates-dev",
        trigger_rule = "none_failed",
    )



        
    with TaskGroup(group_id="Simple_Task_Group") as Simple_Task_Group:
                
        T1 = DummyOperator(
            task_id = "T1",
        )
                
        T2 = DummyOperator(
            task_id = "T2",
        )
                
        T21 = DummyOperator(
            task_id = "T21",
        )
                
        T22 = DummyOperator(
            task_id = "T22",
        )
                
        T3 = DummyOperator(
            task_id = "T3",
        )
                    
        with TaskGroup(group_id="Nested_Task_Group") as Nested_Task_Group:
                    
            T4 = DummyOperator(
                task_id = "T4",
            )
                    
            T5 = DummyOperator(
                task_id = "T5",
            )
                    
            T51 = DummyOperator(
                task_id = "T51",
            )
                    
            T52 = DummyOperator(
                task_id = "T52",
            )
                    
            T6 = DummyOperator(
                task_id = "T6",
            )


            
    Simple_Task_Group >> Nested_Task_Group
    T5 >> T51
    T5 >> T52
    T2 >> T21
    T2 >> T22