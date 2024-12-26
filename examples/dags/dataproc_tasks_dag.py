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
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocUpdateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateWorkflowTemplateOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator


log = logging.getLogger("airflow")
log.setLevel(logging.INFO)



# Define variables
project_id = "composer-templates-dev"
cluster_name = "test-cluster"
region = "us-central1"
create_cluster_config = {"master_config":{"num_instances":1,"machine_type_uri":"n1-standard-4","disk_config":{"boot_disk_type":"pd-standard","boot_disk_size_gb":1024}},"worker_config":{"num_instances":2,"machine_type_uri":"n1-standard-4","disk_config":{"boot_disk_type":"pd-standard","boot_disk_size_gb":1024}},"gce_cluster_config":{"internal_ip_only":1}}
update_cluster_config = {"config":{"worker_config":{"num_instances":3},"secondary_worker_config":{"num_instances":3}}}
hadoop_job_config = {"reference":{"project_id":"composer-templates-dev"},"placement":{"cluster_name":"test-cluster"},"hadoop_job":{"main_jar_file_uri":"file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar","args":["wordcount","gs://pub/shakespeare/rose.txt","us-central1-composer-dag-fa-2d7e71fc-bucket/data/"]}}
spark_job_config = {"reference":{"project_id":"composer-templates-dev"},"placement":{"cluster_name":"test-cluster"},"spark_job":{"jar_file_uris":["file:///usr/lib/spark/examples/jars/spark-examples.jar"],"main_class":"org.apache.spark.examples.SparkPi"}}
create_template_config = {"id":"sparkpi","placement":{"managed_cluster":{"cluster_name":"test-cluster","config":{"master_config":{"num_instances":1,"machine_type_uri":"n1-standard-4","disk_config":{"boot_disk_type":"pd-standard","boot_disk_size_gb":1024}},"worker_config":{"num_instances":2,"machine_type_uri":"n1-standard-4","disk_config":{"boot_disk_type":"pd-standard","boot_disk_size_gb":1024}},"gce_cluster_config":{"internal_ip_only":1}}}},"jobs":[{"spark_job":{"main_class":"org.apache.spark.examples.SparkPi","jar_file_uris":["file:///usr/lib/spark/examples/jars/spark-examples.jar"]},"step_id":"compute"}]}


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
    dag_id='dataproc_tasks_dag',
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
        
    create_cluster = DataprocCreateClusterOperator(
        cluster_config = create_cluster_config,
        cluster_name = cluster_name,
        project_id = project_id,
        region = region,
        task_id = "create_cluster",
        trigger_rule = "none_failed",
    )
        
    update_cluster = DataprocUpdateClusterOperator(
        cluster = update_cluster_config,
        cluster_name = cluster_name,
        deferrable = True,
        project_id = project_id,
        region = region,
        task_id = "update_cluster",
        trigger_rule = "none_failed",
    )
        
    hadoop_job = DataprocSubmitJobOperator(
        deferrable = True,
        job = hadoop_job_config,
        project_id = project_id,
        region = region,
        task_id = "hadoop_job",
        trigger_rule = "none_failed",
    )
        
    spark_job = DataprocSubmitJobOperator(
        deferrable = True,
        job = spark_job_config,
        project_id = project_id,
        region = region,
        task_id = "spark_job",
        trigger_rule = "none_failed",
    )
        
    delete_cluster = DataprocDeleteClusterOperator(
        cluster_name = cluster_name,
        project_id = project_id,
        region = region,
        task_id = "delete_cluster",
        trigger_rule = "none_failed",
    )
        
    create_template = DataprocCreateWorkflowTemplateOperator(
        project_id = project_id,
        region = region,
        task_id = "create_template",
        template = create_template_config,
        trigger_rule = "all_done",
    )
        
    start_template_job = DataprocInstantiateWorkflowTemplateOperator(
        project_id = project_id,
        region = region,
        task_id = "start_template_job",
        template_id = "sparkpi",
        trigger_rule = "none_failed",
    )

    create_cluster >> update_cluster
    update_cluster >> hadoop_job
    update_cluster >> spark_job
    spark_job >> delete_cluster
    delete_cluster >> create_template
    create_template >> start_template_job