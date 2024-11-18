def upload_gcs_to_spanner(
    project_id:str, instance_id:str, database_id:str, bucket_name:str, file_name:str, table_name:str,columns:list):
    """Uploads data from a CSV file in GCS to a Cloud Spanner table."""
    """
    :param str project_id: Google Cloud Project ID
    :param str instance_id: Google Cloud Spanner Instance ID
    :param str database_id: Google Cloud Spanner Database ID
    :param str bucket_name: Google Cloud Storage Bucket to read files
    :param str file_name: Filename to import to 
    :param str table_name: Cloud Spanner Table name for importing data
    :param list columns: Cloud Spanner table column names in ascending order as per DDL (assumed that input file is in same order)
    :return dict : GCS file path and Spanner details
    """

    # Initialize Cloud Spanner client
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Initialize Cloud Storage client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the file from GCS
    data = blob.download_as_string()

    # Parse the CSV data
    rows = []
    for line in data.decode("utf-8").splitlines():
        row = line.split(",")
        rows.append(row)

    # Insert data into Cloud Spanner table
    cols = columns
    with database.batch() as batch:
        batch.insert(table=table_name, columns=cols, values=rows)

    print(f"Data from {file_name} uploaded to {table_name} successfully.")
    return {"input_file_path":"gs://"+f"{bucket_name}"+"/"+f"{file_name}", "spanner_databse_table":f"{database_id}"+":"+f"{table_name}"}

def export_spanner_to_gcs(
    project_id, instance_id, database_id, bucket_name, file_name, sql_query):
    """Exports data from Cloud Spanner to a CSV file in GCS."""
    """
    :param str project_id: Google Cloud Project ID
    :param str instance_id: Google Cloud Spanner Instance ID
    :param str database_id: Google Cloud Spanner Database ID
    :param str bucket_name: Google Cloud Storage Bucket to export files to
    :param str sql_query: Spanner SQL query to fetch data from spanner 
    :return dict : GCS output file path
    """

    # Initialize Cloud Spanner client
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Initialize Cloud Storage client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Execute the query and fetch data
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(sql_query)

    # Format data as CSV
    csv_data = ""
    for row in results:
        csv_data += ",".join(str(value) for value in row) + "\n"

    # Upload the CSV data to GCS
    blob.upload_from_string(csv_data)

    print(f"Data exported to {file_name} in GCS successfully.")
    return {"output_file_path":"gs://"+f"{bucket_name}"+"/"+f"{file_name}"}