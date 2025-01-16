# type: ignore

# Define variables
export_to_gcs_destination_cloud_storage_uris = "gs://hmh_composer_demo/export_files/covid.csv"
export_to_gcs_source_project_dataset_table = "composer-templates-dev.hmh_demo.tmp_covid"
destination_dataset_table = "composer-templates-dev.hmh_demo.tmp_covid"
sql = 'SELECT * FROM `composer-templates-dev.hmh_demo.covid` WHERE case_reported_date = "2021-08-18"'
project_id = "composer-templates-dev"

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