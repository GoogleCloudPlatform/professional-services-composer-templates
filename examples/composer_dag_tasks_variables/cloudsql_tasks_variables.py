# type: ignore

# Define variables
gcp_cloudsql_conn_id = "airflow_composer_template_mysql"
truncate_sales_table_sql = 'TRUNCATE TABLE sales;'
drop_sales_reporting_table_sql = 'DROP TABLE IF EXISTS sales_reporting;'
create_sales_table_sql = 'create table sales_reporting as select s.sale_id, p.product_name, p.description, s.order_date, s.city, s.state, p.price as product_price, s.quantity, (s.quantity*p.price) as actual_sell_price, s.total_price as sell_price, s.total_price - (s.quantity*p.price) as difference from sales s inner join products p on s.product_id = p.product_id;'
composer_instance = "composer-template"

import_from_gcs_cloud_sql_body = {
    "importContext": {
        "fileType": "CSV", 
        "uri": "gs://hmh_composer_demo/demo_test_csv/daily_sales.csv", 
        "database": "transactions", 
        "csvImportOptions": {
            "table": "sales",
            "escapeCharacter":  "5C", 
            "quoteCharacter": "22", 
            "fieldsTerminatedBy": "2C", 
            "linesTerminatedBy": "0A"
        }
    }
}

export_to_gcs_cloud_sql_body = {
    "exportContext":{
        "fileType": "CSV",
        "uri": "gs://hmh_composer_demo/demo_test_csv/sales_report.csv", 
        "databases": ["transactions"], 
        "offload": True, 
        "csvExportOptions": {
            "selectQuery": "SELECT * FROM sales_reporting;"
        }
    }
}


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

