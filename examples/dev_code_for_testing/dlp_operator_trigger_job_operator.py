from airflow import DAG
from airflow.providers.google.cloud.operators.dlp import CloudDLPCreateJobTriggerOperator
from airflow.providers.google.cloud.operators.dlp import CloudDLPCreateDLPJobOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator


# Replace with your project ID
PROJECT_ID = "composer-templates-dev"
# Replace with your GCS bucket and file name
GCS_BUCKET = "composer-templates-dev-input-files"
GCS_FILE = "dlp_operator_input/dlp_operator_sample_data.txt"
# Replace with your DLP de-identification template ID
# DEIDENTIFY_TEMPLATE_ID = "your-deidentify-template-id"


#Payload to create job trigger
# JOB_TRIGGER = {
#     "inspect_job": {
#         "storage_config": {
#             "cloud_storage_options": {
#                 "file_set":{"url": "gs://composer-templates-dev-input-files/dlp_operator_input/"},
#                 "file_types":['TEXT_FILE']
#             }
#         },
#         "inspect_config":{
#             "info_types": [{"name": "PHONE_NUMBER"}, {"name": "US_SOCIAL_SECURITY_NUMBER"},
#                                            {"name": "EMAIL_ADDRESS"}],
#             "min_likelihood": "VERY_UNLIKELY",
#         }
#     },
#     "status": "HEALTHY",
# }

# Payload to trigger DLP Job
trigger_dlp_job_paayload = {
        "storage_config": {
            "cloud_storage_options": {
                "file_set":{"url": "gs://composer-templates-dev-input-files/dlp_operator_input/"},
                "file_types":['TEXT_FILE']
            }
        },
        "inspect_config":{
            "info_types": [{"name": "PHONE_NUMBER"},
                           {"name": "US_SOCIAL_SECURITY_NUMBER"},
                            {"name": "EMAIL_ADDRESS"}],
            "min_likelihood": "VERY_UNLIKELY",
        },
        "inspect_template_name":"projects/composer-templates-dev/locations/global/inspectTemplates/demo-composer-template-inspect-test",
        "actions": [{
            "save_findings":{
                "output_config":{
                    "table": {
                            "project_id": "composer-templates-dev",
                            "dataset_id": "test_dataset",
                            "table_id": "dlp_googleapis_2024_11_02_5447016892596828032"
                        }
                }
                }
            }
            ,{
            "deidentify":{
                "transformation_config":{
                    "deidentify_template":"projects/composer-templates-dev/locations/global/deidentifyTemplates/demo-composer-template-test"
                },
                "cloud_storage_output":"gs://hmh_backup/deidentified-data_output/"
            }
            }
        ]
    }


    
with DAG(
    "dl_create_job_trigger",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["dlp"],
) as dag:
    
    start = DummyOperator(task_id='start')

    # trigger_job = CloudDLPCreateJobTriggerOperator(
    #     project_id=PROJECT_ID,
    #     job_trigger=JOB_TRIGGER,
    #     trigger_id="test-inspect-deidentify-file-run-now-from-airflow",
    #     task_id="trigger_dlp_job",
    # )

    # trigger_job = CloudDLPCreateDLPJobOperator(
    #     project_id=PROJECT_ID,
    #     inspect_job=trigger_dlp_job_paayload,
    #     task_id="trigger_dlp_job",
    # )

    start >> trigger_job