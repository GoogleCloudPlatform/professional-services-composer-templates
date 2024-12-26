# type: ignore

# Define variables
gcp_project_id = "composer-templates-dev"
dlp_job_payload = {"storage_config": {"cloud_storage_options": {"file_set": {"url": "gs://composer-templates-dev-input-files/dlp_operator_input/"}, "file_types": ["TEXT_FILE"]}}, "inspect_config": {"info_types": [{"name": "PHONE_NUMBER"}, {"name": "US_SOCIAL_SECURITY_NUMBER"}, {"name": "EMAIL_ADDRESS"}], "min_likelihood": "VERY_UNLIKELY"}, "inspect_template_name": "projects/composer-templates-dev/locations/global/inspectTemplates/demo-composer-template-inspect-test", "actions": [{"save_findings": {"output_config": {"table": {"project_id": f"{gcp_project_id}", "dataset_id": "test_dataset", "table_id": "dlp_googleapis_2024_11_02_5447016892596828032"}}}}, {"deidentify": {"transformation_config": {"deidentify_template": "projects/composer-templates-dev/locations/global/deidentifyTemplates/demo-composer-template-test"}, "cloud_storage_output": "gs://hmh_backup/deidentified-data_output/"}}]}


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

