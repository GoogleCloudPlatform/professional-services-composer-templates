# Google Cloud Composer Templates
Google Cloud Composer is the de facto data processing orchestration engine in Google Cloud Platform. Orchestration is the core of any data pipeline. This solution allows customers to easily launch templates without needing extensive expertise in cloud composer. This also simplifies the process of loading, processing, scheduling, and orchestrating jobs within the Google Cloud Platform.

#### Disclaimer
This is not an officially supported Google product. Please be aware that bugs may lurk, and that we reserve the right to make small backwards-incompatible changes. Feel free to open bugs or feature requests, or contribute directly
(see [CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/composer-templates/blob/main/CONTRIBUTING.md) for details).


## Install requirements
```sh
python3 -m pip install -r requirements.txt 
```



## YAML Sections
- **(Mandatory) DAG parameters**: Defines general DAG-level settings like dag_id, description, schedule, etc.

```yaml
dag_id: cloudspanner_import_export_tasks_dag     
description: "Sample example to import/export data in and out of cloud spanner."
max_active_runs:
catchup: False
schedule: None
tags: ["spanner","test","v1.1"]
start_date:
end_date:
max_active_tasks: 5
dagrun_timeout: timedelta(hours=3)
is_paused_upon_creation: True
...
```

- **(Optional) additional_imports**: List imports required outside of task operators

```yaml
# Optional
additional_imports:
  - module: from google.cloud import spanner, storage
  - module: import json
```

- **(Optional) task_variables**: Location of the Python file defining variables and `default_args` for the `DAG` object.

```yaml
task_variables:
  variables_file_path: examples/composer_dag_tasks_variables/cloudspanner_tasks_variables.py
```

- **(Optional) custom_python_functions**: Allows you to define custom Python functions to be used within the DAG. 
  - **Import from file**:  Python functions can be imported directly from file by setting `import_functions_from_file: True`

    ```yaml
    custom_python_functions:
      import_functions_from_file: True
      functions_file_path: examples/python_function_files/cloudspanneroperator_python_functions.py
    ```

  - **Define in YAML**: Or, python functions can be directly defined in the YAML by setting `import_functions_from_file: False`

    ```yaml
    custom_python_functions:
      import_functions_from_file: False
      custom_defined_functions:
        upload_gcs_to_spanner:
          description: Uploads data from a CSV file in GCS to a Cloud Spanner table.
          code: |
            def print_args(*args):
              """Prints all arguments passed to the function.

              Args:
                *args: Any number of arguments.
              """
              for arg in args:
                print(arg)
    ```
- **(Optional) tasks**: Contains the definitions of individual tasks within the DAG. Each task is defined with properties like task_id, task_type, and task-specific configurations.

```yaml
tasks:
  - task_id: gcs_to_spanner
    task_type: airflow.operators.python_operator.PythonOperator
    python_callable: upload_gcs_to_spanner
    op_kwargs: {"project_id":gcp_project_id,"instance_id":spanner_instance_id,"database_id":spanner_databse_id,"bucket_name":import_gcs_bucket_name,"file_name":import_gcs_file_name,"table_name":spanner_table,"columns":spanner_table_columns}
    trigger_rule : 'all_done'
    depends_on: 
      - extract_data
  - task_id: delete_results_from_spanner
    task_type: airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator
    instance_id: spanner_instance_id
    database_id: spanner_databse_id
    query: spanner_sql_query 
    project_id: gcp_project_id
    trigger_rule : 'all_done'
    depends_on: 
      - gcs_to_spanner
```

- **(Optional) task_groups**: Contains the definitions of Airflow TaskGroups within the DAG. 
  - Refer  [Example: Nested TaskGroup](examples/dag_configs/nested_task_groups.yaml)
  - Refer [Example: TaskGroup with `for` loops and `if` conditions](examples/dag_configs/taskgroup_with_loops_and_conditions.yaml)

```yaml
task_groups:
  - group_id: production_tasks
    depends_on:
      - get_config
    tasks:
      - task_id: f'process_data_{i}'
        pre_condition: |
          # Use a for loop to create multiple tasks dynamically
          for i in range(3):  # You can use a variable here if needed
        task_type: airflow.operators.python.PythonOperator
        python_callable: process_data
        op_kwargs: "{'task_number': f'{i}'}"
        passthrough_keys:
          - op_kwargs
      - task_id: finalize_production
        task_type: airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator
        sql: SELECT CURRENT_TIMESTAMP() AS completion_time;
        use_legacy_sql: False
```

- **(Optional) task_dependency**: Controls the default task dependency behavior. All tasks with dependencies are validated against the YAML-defined tasks, regardless of the scenario
  - **Default**: Dependency will be dynamically built based on `depends_on` in your task definitions

    ```yaml
    task_dependency:
      default_task_dependency: True
    ```

  - **Custom**: User provided dependency.

    ```yaml
    task_dependency:
      default_task_dependency: True
      custom_task_dependency: 
        - "my_task_group1 >> extract_data"
        - "bash_example1 >> extract_from_source_a"
        - "bash_example2 >> extract_from_source_b"
        - "extract_data >> gcs_to_spanner >> delete_results_from_spanner >> [spanner_to_gcs, sample_python]"
    ```


## Examples
- Refer [YAML - DAG Configuration examples](examples/dag_configs/)
- Refer [Output - Airflow DAGs](examples/dags)

## DAG Generation
### Step 1: Create a configuration file as shown below
- Refer [YAML - DAG Configuration examples](examples/dag_configs/)

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

### Step 3: Deploy generated DAG to your composer environment

To deploy the DAG, please provide the following command-line arguments tailored to your Google Cloud Composer enviornment.

Below parameters are supplied for deployment

| Parameter | Description |
| :---     | :--- |
| gcp_project_id | Google Cloud Platform Project ID |
| gcp_composer_env_id | Google Cloud Composer Environment Name  
| composer_env_location | Google Cloud Composer Environment location
| dag_file | Full file path of the python DAG file to be uploaded E.g '/Users/abc/main/dags/cloudsql_tasks_dag.py'  
| tasks_variables_flag | Tasks Variable flag which states that the DAG read variables from use_case_tasks_variables.yaml file allowed values are True or False  
| tasks_variables_file | Full file path of the YAML task variables file to be uploaded E.g '/examples/composer_dag_tasks_variables/cloudspanner_tasks_variables.yaml'

The command serves as an example, please update the parameters values as per your Google Cloud Platform configurations. 

```sh
 python3 /source/deploy_dag.py \
 -gcp_project_id='composer-templates' \
 -gcp_composer_env_name='composer-templates' \
 -composer_env_location='us-central1' \
 -dag_file='/examples/dags/cloudspanner_import_export_tasks_dag.py' \
 -tasks_variables_flag=False \
 -tasks_variables_file='/examples/composer_dag_tasks_variables/cloudspanner_tasks_variables.yaml'
```


----
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