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
| PythonOperator     |  Creates and runs custom defined Python Functions. |
| SpannerQueryDatabaseInstanceOperator     |  Executes an arbitrary DML query (INSERT, UPDATE, DELETE) on Cloud Spanner. |


### DAG Generation
#### Step 1: Create a configuration file as shown below

```yaml
---
# DAG parameters
dag_id: bigquery_tasks_dag    # mandatory 
catchup: False
schedule_interval: '@hourly'  # mandatory; enclose values within quotes       
tags: ["test"]

default_args: # mandatory
    owner: 'test'
    email_on_failure: False
    email_on_retry: False
    retry_delay: 30  # seconds               

#Python functions used by Python operator. If there are no function keep it blank
functions:
  perform_transformation:
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


# Environment specific configs
# mandatory - Having a default envs is mandatory.
# All environment specific variables should be named starting with cc_var_
envs:
    default:
        # DAG tasks configs
        # mandatory
        tasks:
        - task_id: create_bq_dataset
          task_type: BigQueryCreateEmptyDatasetOperator
          dataset_id: 'test_dataset'
          # Environment specific variable
          project_id: cc_var_project_id
          upstream_task: None
          trigger_rule : 'none_failed'
        #cc_operator_description: print the GCS uri where the file is written.
        - task_id: print_gcs_uri
          task_type: airflow.operators.python_operator.PythonOperator
          python_callable: pull_xcom
          trigger_rule : 'all_done'
          upstream_task: export_sales_reporting_table_to_gcs
    composer-templates-dev: # This should match the composer environment name in GCP
      cc_var_project_id: composer-templates-dev
      # You can use '{{var.value.<variable_name>}}' to retrieve any environment variable value declared under each composer environment.
      # To read and execute Bash script via BashOperator add the path as shown below
      # Add your scripts to your composer dag folder on GCS, example gs://composer-templates-dev/dags/scripts while adding this task in YAML file\
      # add the path for bash script as /home/airflow/gcs/dags/scripts as gs://composer-templates-dev/dags translates to /home/airflow/gcs/dags when deployed
      # Furthremore, add an extra SPACE after the script path before the end quotes as shown below else BashOperator will throw an error
      cc_var_bash_script: "bash /home/airflow/gcs/dags/scripts/sample_bash_script.sh "

```

#### Step 2: Run the template to generate the DAG .py file

To generate the DAG, please provide the following command-line arguments tailored to your specific use case:

| Argument | Description |
| :---     | :--- |
| --config_file (mandatory)     |  YAML configuration file location. |
| --dag_template |  Template to consume for DAG generation - default template is standard_dag.template |
| --dynamic_config (mandatory)     |  Is this DAG dynamically configurable at runtime? Yes. Values for DAG parameters are pulled from the relevant composer environment section of the YAML configuration file. For dynamic configuration, this YAML file needs to be copied to the "dag_configs" folder in Google Cloud Storage (GCS). Please note that this folder will need to be created if it's the first time you're using it. |
| --composer_env_name (mandatory when dynamic_config = false) |  When dynamic_config is set to false, configuration values will be retrieved from the specified composer environment section of the YAML. If the YAML file lacks environment-specific variables, use the command-line argument --composer_env_name with the value default |

Below code samples will produce a Python file for the DAG, using the same name as the dag_id specified within the YAML file. 

#### Case 1: 
When dynamic_config is set to false, all DAG parameter values are fetched from the envs:default section of the YAML file. In this scenario, no DAG parameter values can include variables (those starting with cc_var_). The --composer_env_name for this use case should always be default.

```sh
python3 source/generate_dag.py --config_file examples/dag_configs/bigquery_tasks_config.yaml --dynamic_config false --composer_env_name default
```

#### Case 2: 
When dynamic_config is false but DAG parameter values contain variables (like cc_var_), the corresponding values will be sourced from the appropriate composer environment section within the YAML file. 

```sh
python3 source/generate_dag.py --config_file examples/dag_configs/bigquery_tasks_config.yaml --dynamic_config false --composer_env_name composer-templates-dev
```

#### Case 3: 
DAG parameter values are retrieved during run-time

```sh
python3 source/generate_dag.py --config_file examples/dag_configs/bigquery_tasks_config.yaml --dynamic_config true
```
### DAG Deployment
Use the following command to deploy generated DAG and its respective configuration file onto your hosted Google Cloud Composer enviornment.

Below parameters are supplied for deployment

| Parameter | Description |
| :---     | :--- |
| gcp_project_id | Google Cloud Platform Project ID |
 gcp_composer_env_id | Google Cloud Composer Environment Name  
| composer_env_location | Google Cloud Composer Environment location
| dag_file | Full file path of the python DAG file to be uploaded E.g '/Users/abc/main/dags/cloudsql_tasks_dag.py'  
| dynamic_dag_flag | Dynamic DAG flag which states that the DAG uses config file allowed values are True or False  
| dynamic_dag_config_file | Full file path of the YAML DAG config file to be uploaded E.g 'Users/abc/main/dag_configs/cloudsql_tasks_config.yaml'

The command serves as an example, please update the parameters values as per your Google Cloud Platform configurations.  

```sh
 python3 ./source/deploy_dag.py \
 -gcp_project_id='composer-templates-dev' \
 -gcp_composer_env_name='composer-templates-dev' \
 -composer_env_location='us-central1' \
 -dag_file='/Users/abc/main/dags/cloudsql_tasks_dag.py' \
 -dynamic_dag_flag=False \
 -dynamic_dag_config_file='/Users/abc/main/dag_configs/cloudsql_tasks_config.yaml'
 ```
