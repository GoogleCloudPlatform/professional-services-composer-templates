from airflow import DAG
from airflow.providers.google.cloud.operators.dlp import CloudDLPDeidentifyContentOperator
from airflow.utils.dates import days_ago
from google.cloud.dlp_v2.types import ContentItem, DeidentifyTemplate, InspectConfig
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

# Replace with your project ID
PROJECT_ID = "composer-templates-dev"
# Replace with your GCS bucket and file name
GCS_BUCKET = "composer-templates-dev-input-files"
GCS_FILE = "dlp_operator_input/dlp_operator_sample_data.txt"
# Replace with your DLP de-identification template ID
# DEIDENTIFY_TEMPLATE_ID = "your-deidentify-template-id"

# Sample JSON data with sensitive information (replace with your actual data)
# SAMPLE_DATA = """
# [
#   {
#     "name": "John Doe",
#     "email": "john.doe@example.com",
#     "phone": "555-123-4567"
#   },
#   {
#     "name": "Jane Smith",
#     "email": "jane.smith@example.com",
#     "phone": "555-987-6543"
#   }
# ]
# """

INSPECT_CONFIG = InspectConfig(info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_SOCIAL_SECURITY_NUMBER"},
                                           {"name": "EMAIL_ADDRESS"}])

# DEIDENTIFY_CONFIG = {
#     "info_type_transformations": {
#         "transformations": [
#             {
#                 "primitive_transformation": {
#                     "replace_config": {"new_value": {"string_value": "[deidentified_number]"}}
#                 }
#             }
#         ]
#     }
# }


DEIDENTIFY_CONFIG = {
    "info_type_transformations": {
        "transformations": [
            {
                "primitive_transformation": {
                    "character_mask_config": {"masking_character":"#","reverse_order":None}
                }
            }
        ]
    }
}



# ITEM = ContentItem(
#     table={
#         "headers": [{"name": "column1"}],
#         "rows": [{"values": [{"string_value": "My phone number is (206) 555-0123"}]}],
#     }
# )

ITEM = ContentItem(
    value="""[
                {
                    "name": "John Doe",
                    "email": "john.doe@example.com",
                    "phone": "555-123-4567",
                    "ssn": "555-23-4567"
                },
                {
                    "name": "Jane Smith",
                    "email": "jane.smith@example.com",
                    "phone": "555-987-6543",
                    "ssn": "587-87-6543"
                }
            ]""")



def read_file_content():
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket("composer-templates-dev-input-files")
    blob = bucket.blob("dlp_operator_input/dlp_operator_sample_data.txt")
    contents = blob.download_as_bytes()
    # Decode the contents if necessary
    contents = contents.decode('utf-8') if isinstance(contents, bytes) else contents
    print(contents)
    return contents

    
with DAG(
    "dlp_deidentify_content_test",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["dlp"],
) as dag:
    
    read_content = PythonOperator(
        task_id='read_file_content',
        python_callable=read_file_content,
    )

    deidentify_content = CloudDLPDeidentifyContentOperator(
        project_id=PROJECT_ID,
        item= ITEM,
        deidentify_config=DEIDENTIFY_CONFIG,
        inspect_config=INSPECT_CONFIG,
        task_id="deidentify_content",
    )

    read_content >> deidentify_content