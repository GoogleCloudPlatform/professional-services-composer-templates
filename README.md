# Google Cloud Composer Templates
Google Cloud Composer is the de facto data processing orchestration engine in Google Cloud Platform. Orchestration is the core of any data pipeline. This solution allows customers to easily launch templates without needing extensive expertise in cloud composer. This also simplifies the process of loading, processing, scheduling, and orchestrating jobs within the Google Cloud Platform.

#### Disclaimer
This is not an officially supported Google product. Please be aware that bugs may lurk, and that we reserve the right to make small backwards-incompatible changes. Feel free to open bugs or feature requests, or contribute directly
(see [CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/composer-templates/blob/main/CONTRIBUTING.md) for details).


## Install requirements
```sh
python3 -m pip install -r requirements.txt 
```


## Usage
The solution has undergone pre-testing with a variety of operators, as showcased in the examples/dag_configs folder. 

Here's a list of the operators that have been pretested:

| Operator | Description |
| :---     | :--- |
| BigQueryCreateEmptyDatasetOperator     | Create a new dataset for your Project in BigQuery. |
| BigQueryCreateEmptyTableOperator     | Creates a new table in the specified BigQuery dataset, optionally with schema. |
| BigQueryGetDataOperator     | Fetch data and return it, either from a BigQuery table, or results of a query job. |
| BigQueryToGCSOperator     |  Transfers a BigQuery table to a Google Cloud Storage bucket. |
| BigQueryExecuteQueryOperator     |  Executes BigQuery SQL queries in a specific BigQuery database. |
| CloudSQLExecuteQueryOperator     |  Performs DDL or DML SQL queries in Google Cloud SQL instance. |
| CloudSQLImportInstanceOperator     |  Import data into a Cloud SQL instance from Cloud Storage. |
| CloudSQLExportInstanceOperator     |  Export data from a Cloud SQL instance to a Cloud Storage bucket. |
| CountTokensOperator     |  Pre-determine the number of tokens of a provided prompt. |
| DataflowTemplatedJobStartOperator     |  Start a Templated Cloud Dataflow job; the parameters of the operation will be passed to the job. |
| DataflowStartFlexTemplateOperator     |  Starts flex templates with the Dataflow pipeline. |
| BeamRunJavaPipelineOperator     |  Starts dataflow Java job |
| CloudDataFusionStartPipelineOperator     |  Starts a datafusion pipeline |
| CloudDLPCreateDLPJobOperator     |  Creates a new job to inspect storage or calculate risk metrics for sensitive data and masks sensitive values. |
| DataflowStopJobOperator     |  Stops the job with the specified name prefix or Job ID. |
| DataformCreateCompilationResultOperator     |  Creates a new CompilationResult in a given project and location. |
| DataformCreateWorkflowInvocationOperator     |  Creates a new WorkflowInvocation in a given Repository. |
| DataprocCreateClusterOperator     |  Create a new cluster on Google Cloud Dataproc. |
| DataprocUpdateClusterOperator     |  Update a cluster in a project. |
| DataprocSubmitJobOperator     |  Submit a job to a cluster. |
| DataprocDeleteClusterOperator     |  Delete a cluster in a project. |
| DataprocCreateWorkflowTemplateOperator     |  Creates new workflow template. |
| DataprocInstantiateWorkflowTemplateOperator     |  Instantiate a WorkflowTemplate on Google Cloud Dataproc. |
| GCSCreateBucketOperator     |  Creates a new bucket. |
| GCSSynchronizeBucketsOperator     |  Synchronizes the contents of the buckets or bucket's directories in the Google Cloud Services. |
| GCSListObjectsOperator     |  List all objects from the bucket filtered by given string prefix and delimiter in name or match_glob. |
| GCSDeleteBucketOperator     |  Deletes bucket from a Google Cloud Storage. |
| GCSObjectsWithPrefixExistenceSensor     |  Checks for the existence of GCS objects at a given prefix, passing matches via XCom. |
| GCSDeleteObjectsOperator     |  Deletes objects from a list or all objects matching a prefix from a Google Cloud Storage bucket. |
| GCSDeleteObjectsOperator     |  Deletes objects from a list or all objects matching a prefix from a Google Cloud Storage bucket. |
| GenerativeModelGenerateContentOperator     |  Generate content via a Generative Model like Google Gemini. |
| PythonOperator     |  Creates and runs custom defined Python Functions. |
| SpannerQueryDatabaseInstanceOperator     |  Executes an arbitrary DML query (INSERT, UPDATE, DELETE) on Cloud Spanner. |
| SupervisedFineTuningTrainOperator     |  Fine-tune a generative model and deploy it to an endpoint. |


## YAML Sections
1. **DAG parameters**: Defines general DAG-level settings like dag_id, description, schedule_interval, etc.
2. **default_args**: Specifies default arguments to be applied to all tasks in the DAG, such as owner, retries, and email notifications.
3. **custom_python_functions**: Allows you to define custom Python functions to be used within the DAG. 
4. **tasks**: Contains the definitions of individual tasks within the DAG. Each task is defined with properties like task_id, task_type, and task-specific configurations.
5. **task_dependency**: Controls the default task dependency behavior.
6. **task_variables**: Manages the loading and defining of variables used within the DAG. You can either import variables from a YAML file in GCS or define them directly within this section.


## DAG Generation
### Step 1: Create a configuration file as shown below

```yaml
---
# DAG parameters
dag_id: bigquery_tasks_dag    # [Mandatory] 
catchup: False
schedule_interval: '@hourly'  # [Mandatory] Enclose values within quotes       
tags: ["test"]


default_args: # [Mandatory]
    owner: 'test'
    email_on_failure: False
    email_on_retry: False
    retry_delay: 30  # seconds               


# Define Python functions to be added in your Airflow DAG
# - import_functions_from_file:  
#   - True:  Load functions from a local Python file (specify the 'file_path').
#   - False: Define functions directly within this YAML configuration.
# - functions: In-place code.
custom_python_functions:
  # Option 1 - Import your custom python functions from a file
  # NOTE: Users need to ensure the validty of Python function mentioned in the file
  import_functions_from_file: False
  functions_file_path: <YOUR LOCAL DIRECTORY>/cloudspanneroperator_python_functions.txt

  # Option 2 - Define it directly in the configuration file here
  custom_defined_functions:
    upload_gcs_to_spanner:
    description: Sample function as an example to perform custom transformation.
    code: |
      def transformation(data):
        """
        Sample function as an example to perform custom transformation.

        Args:
          data: Sample data on which we can perform any transformation.
        
        Returns:
          The data converted into a string format.
        """
        print("Printing sample payload from transformation function: {}".format(data))
        output = str(data)
        return output
  pull_xcom:
    description: Function to pull xcom variables from export GCS task print or use it for other transformations.
    code: |
      def pull_xcom(**kwargs):
        """
        Pulls a value from XCom and prints it.
        """
        ti = kwargs['ti']
        pulled_value = str(ti.xcom_pull(task_ids='export_sales_reporting_table_to_gcs', key='file_details'))
        print(f"Pulled value from XCom: {pulled_value}")
        return pulled_value


# Tasks specific configs
# mandatory
tasks:
  - task_id: gcs_to_spanner
    task_type: airflow.operators.python_operator.PythonOperator
    python_callable: upload_gcs_to_spanner
    op_kwargs: {"project_id":cc_var_gcp_project_id,"instance_id":cc_var_spanner_instance_id,"database_id":cc_var_spanner_databse_id,"bucket_name":cc_var_import_gcs_bucket_name,"file_name":cc_var_import_gcs_file_name,"table_name":cc_var_spanner_table,"columns":cc_var_spanner_table_columns}
    trigger_rule : 'all_done'
    depends_on: None
  - task_id: delete_results_from_spanner
    task_type: airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator
    instance_id: cc_var_spanner_instance_id
    database_id: cc_var_spanner_databse_id
    query: cc_var_spanner_sql_query 
    project_id: cc_var_gcp_project_id
    trigger_rule : 'all_done'
    depends_on: gcs_to_spanner


task_dependency:
  # Controls task dependency management.
  # - default_task_dependency: Controls default dependency behavior.
  #   - If True, tasks will depend on each other based on the `depends_on` configuration within each task definition.
  #   - If False, use `custom_task_dependency` to define dependencies.
  # - custom_task_dependency:  Explicitly define dependencies
  
  # Examples (Each entry `-` defines a separate dependency chain in the generated Airflow DAG. Must be enclosed with double-quotes):
  #   - "task_a >> task_b >> task_c"  
  #   - "[task_a, task_b] >> task_c" 
  #   - "task_a >> task_b | task_c >> task_d" 
  default_task_dependency: False
  custom_task_dependency: 
    - "start >> gcs_to_spanner >> delete_results_from_spanner >> spanner_to_gcs"
    - "[start >> gcs_to_spanner] >> spanner_to_gcs"
    - "start >> [delete_results_from_spanner, spanner_to_gcs]"


task_variables:
  # Load variables from a YAML file in GCS. The file should be at:
  # gs://<YOUR_COMPOSER_ENV_NAME>/dag_variables/<variables_file_name>
  import_from_file: True  # Boolean to enable/disable file import
  file_name: cloudspanner_tasks_variables.yaml
  environment: dev  # Environment to load from the file

  # Define variables here if not importing from file.
  variables:
    cc_var_gcp_project_id: composer-templates-dev
    cc_var_spanner_instance_id: composer-templates-spannerdb
    cc_var_spanner_databse_id: dev-spannerdb
```

### Step 2: Run the template to generate the DAG .py file

To generate the DAG, please provide the following command-line arguments tailored to your specific use case:

| Argument | Description |
| :---     | :--- |
| --config_file (mandatory)     |  Template configuration YAML file location |
| --dag_template (optional) |  Template to use for DAG generation. Default template is `standard_dag.template` |

Below code samples will produce a Python file for the DAG, using the same name as the dag_id specified within the YAML file. 

```sh
python3 source/generate_dag.py --config_file examples/dag_configs/cloudspanner_import_export_config.yaml
```