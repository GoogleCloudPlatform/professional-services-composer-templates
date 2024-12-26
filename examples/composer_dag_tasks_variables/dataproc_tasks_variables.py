# type: ignore

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