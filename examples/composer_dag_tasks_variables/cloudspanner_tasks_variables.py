# type: ignore

# Define variables
gcp_project_id = "composer-templates-dev"
spanner_instance_id = "composer-templates-spannerdb"
spanner_databse_id = "dev-spannerdb"
spanner_table = "Products"
spanner_table_columns = ["ProductId","ProductName","Description","Price","LastModified"]
spanner_sql_query = "DELETE FROM Products WHERE LastModified < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 DAY);"
import_gcs_bucket_name = "composer-templates-dev-input-files"
import_gcs_file_name = "spanner_input/sample_data_spanner.csv"
export_gcs_bucket_name = "hmh_backup"
export_gcs_file_name = "spanner_output/product_data_output.csv"
spanner_sql_export_query = "SELECT * FROM Products ;"

# Define Airflow DAG default_args
default_args = {
    "owner": 'test',
    "depends_on_past": False,
    "retries": 3,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ['test@example.com'],
    "retry_delay": timedelta(minutes=1),
    "mode": 'reschedule',
    "poke_interval": 120,
    "sla": timedelta(minutes=60),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=60)
}