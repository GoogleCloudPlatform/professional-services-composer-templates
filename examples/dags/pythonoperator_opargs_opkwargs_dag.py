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
from datetime import datetime
from airflow.operators.python import PythonOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)


def greet(name):
  print(f"Hello, {name}!")


# Define variables
alpha = 'alpha'
beta = 'beta'


dag = DAG(
    dag_id='pythonoperator_opargs_opkwargs_dag',
    schedule=None,
    description='test dag',
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=6),
    tags=['composer-templates'],
    start_date=datetime(2024, 12, 20),
    end_date=datetime(2024, 12, 1),
    
)


with dag:
        
    greet_task = PythonOperator(
        op_kwargs = {
          'name': f'{alpha}',
          'test': 'val'
        },
        python_callable = greet,
        task_id = "greet_task",
    )
        
    with TaskGroup(group_id="greet_group") as greet_group:
                
        names = ['Alpha', 'Beta']
        for name in names:
            f'greet_{name}' = PythonOperator(
                op_args = [name],
                python_callable = greet,
                task_id = f'greet_{name}',
            )

            