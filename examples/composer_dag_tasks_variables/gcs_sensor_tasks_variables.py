# type: ignore

# Define variables
gcs_bucket_name = "sample-bq-test-2"
gcs_bucket_prefix = "gcs-sensor/"
bash_command = 'echo {{ ti.xcom_pull(task_ids="gcs_file_sensor") }}'


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