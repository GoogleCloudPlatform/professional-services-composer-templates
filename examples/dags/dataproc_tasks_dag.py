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
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocUpdateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateWorkflowTemplateOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator

log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

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
        dag_id='dataproc_tasks_dag',
        default_args = default_args,
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=True,
        tags=['test'],
        start_date=airflow.utils.dates.days_ago(0)
)

with dag:

    start = DummyOperator(task_id='start')

    create_cluster = DataprocCreateClusterOperator (
            task_id = 'create_cluster',
            cluster_name = "test-cluster",
            region = "us-central1",
            project_id = "composer-templates-dev",
            cluster_config = {"gce_cluster_config": {"internal_ip_only": 1}, "master_config": {"disk_config": {"boot_disk_size_gb": 1024, "boot_disk_type": "pd-standard"}, "machine_type_uri": "n1-standard-4", "num_instances": 1}, "worker_config": {"disk_config": {"boot_disk_size_gb": 1024, "boot_disk_type": "pd-standard"}, "machine_type_uri": "n1-standard-4", "num_instances": 2}},
            trigger_rule = 'none_failed',
        )

    update_cluster = DataprocUpdateClusterOperator (
            task_id = 'update_cluster',
            cluster_name = "test-cluster",
            project_id = "composer-templates-dev",
            region = "us-central1",
            graceful_decommission_timeout = {'seconds': 600},
            cluster = {"config": {"secondary_worker_config": {"num_instances": 3}, "worker_config": {"num_instances": 3}}},
            update_mask = {'paths': ['config.worker_config.num_instances', 'config.secondary_worker_config.num_instances']},
            deferrable = True,
            trigger_rule = 'none_failed',
        )

    hadoop_job = DataprocSubmitJobOperator (
            task_id = 'hadoop_job',
            region = "us-central1",
            project_id = "composer-templates-dev",
            job = {"hadoop_job": {"args": ["wordcount", "gs://pub/shakespeare/rose.txt", "us-central1-composer-dag-fa-2d7e71fc-bucket/data/"], "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar"}, "placement": {"cluster_name": "test-cluster"}, "reference": {"project_id": "composer-templates-dev"}},
            deferrable = True,
            trigger_rule = 'none_failed',
        )

    spark_job = DataprocSubmitJobOperator (
            task_id = 'spark_job',
            region = "us-central1",
            project_id = "composer-templates-dev",
            job = {"placement": {"cluster_name": "test-cluster"}, "reference": {"project_id": "composer-templates-dev"}, "spark_job": {"jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"], "main_class": "org.apache.spark.examples.SparkPi"}},
            deferrable = True,
            trigger_rule = 'none_failed',
        )

    delete_cluster = DataprocDeleteClusterOperator (
            task_id = 'delete_cluster',
            cluster_name = "test-cluster",
            region = "us-central1",
            project_id = "composer-templates-dev",
            trigger_rule = 'none_failed',
        )

    create_template = DataprocCreateWorkflowTemplateOperator (
            task_id = 'create_template',
            template = {"id": "sparkpi", "jobs": [{"spark_job": {"jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"], "main_class": "org.apache.spark.examples.SparkPi"}, "step_id": "compute"}], "placement": {"managed_cluster": {"cluster_name": "test-cluster", "config": {"gce_cluster_config": {"internal_ip_only": 1}, "master_config": {"disk_config": {"boot_disk_size_gb": 1024, "boot_disk_type": "pd-standard"}, "machine_type_uri": "n1-standard-4", "num_instances": 1}, "worker_config": {"disk_config": {"boot_disk_size_gb": 1024, "boot_disk_type": "pd-standard"}, "machine_type_uri": "n1-standard-4", "num_instances": 2}}}}},
            project_id = "composer-templates-dev",
            region = "us-central1",
            trigger_rule = 'all_done',
        )

    start_template_job = DataprocInstantiateWorkflowTemplateOperator (
            task_id = 'start_template_job',
            template_id = 'sparkpi',
            project_id = "composer-templates-dev",
            region = "us-central1",
            trigger_rule = 'none_failed',
        )
    
    start >> create_cluster >> update_cluster >> [hadoop_job,spark_job] >> delete_cluster >> create_template >> start_template_job