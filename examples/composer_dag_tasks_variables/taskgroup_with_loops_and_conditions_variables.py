# type: ignore

# Define variables
PROJECT_ID = 'your-gcp-project-id'  # Replace with your actual project ID
DATAPROC_CLUSTER = 'your-dataproc-cluster'  # Replace with your Dataproc cluster name
GCS_BUCKET = 'your-gcs-bucket'  # Replace with your GCS bucket name
REGION = 'your-gcp-region'  # Replace with your GCP region

# Define Airflow DAG default_args
default_args = {
    "depends_on_past": False,
    "email": 'test@example.com',
    "email_on_failure": False,
    "email_on_retry": False,
    "owner": 'test',
    "start_date": datetime(2024, 12, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=2), 
    "sla": timedelta(minutes=55),
    "execution_timeout": timedelta(minutes=60)
}