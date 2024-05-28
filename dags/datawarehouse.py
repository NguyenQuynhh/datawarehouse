# from datetime import timedelta, datetime
# from airflow import DAG
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# # from clustering import train_model 


# # os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/opt/airflow/config/gcloud_service_key.json' ## đã chỉnh về project củ mình, nhưng k hiểu

# #ELT của anh Tuấn Thành

# #DAG arguments 
# default_args = {
#     'owner': 'ngquynh',
#     'depends_on-past': False,
#     # 'start_date'= datetime(2024, 5, 27), 
#     'email on failure': False,
#     'email_on_retry': False,
#     'retries': None,
#     'retry_delay': timedelta(minutes=2),}

# #DAG definition

# dag = DAG(
#     'oltp_to_olap_transform',
#     default_args=default_args,
#     description='Transform OLTP to OLAP schema in BigQuery',
#     template_searchpath='/opt/airflow/dags/SQL_Queries', ### không hiểu
#     schedule_interval=None,
#     start_date=datetime(2024, 5, 27),
# )

# # Task to load data from GCS to BigQuery 
# # category
# load_categories_gcs_to_bq = GCSToBigQueryOperator(
#     task_id='load_categories_to_bq',
#     bucket='warehouse8',
#     source_objects=['dataupdated/categories.csv'],  # Can be a list of files or a wildcard pattern
#     destination_project_dataset_table='datawarehouse-423912:warehouse.categories',
#     source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
#     write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
#     skip_leading_rows=1,  # Skip header row if CSV
#     schema_fields=[
#         {'name': 'categoryID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#         {'name': 'categoryName', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
#         # Add schema fields as per your data structure
#     ],
#     gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
#     max_bad_records=10,  # Allow up to 10 bad records before failing
#     field_delimiter=';',  # Specify the field delimiter
#     ignore_unknown_values=True, 
#     dag=dag  
# )


# load_categories_gcs_to_bq


