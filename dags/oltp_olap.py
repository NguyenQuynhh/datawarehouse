# from datetime import timedelta, datetime
# from airflow import DAG
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.operators.bash_operator import BashOperator

# # from clustering import train_model 


# # os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/opt/airflow/config/gcloud_service_key.json' ## đã chỉnh về project củ mình, nhưng k hiểu



# #DAG arguments 
# default_args = {
#     'owner': 'ngquynh',
#     'depends_on-past': False,
#     'email on failure': False,
#     'email_on_retry': False,
#     'retries': None,
#     'retry_delay': timedelta(minutes=2),}

# #DAG definition

# dag = DAG(
#     'oltp_to_olap_all_transform',
#     default_args=default_args,
#     description='Transform OLTP to OLAP schema in BigQuery',
#     template_searchpath='/opt/airflow/dags/SQL_Queries', ### không hiểu
#     schedule_interval=None,
#     start_date=datetime(2024, 5, 28),
# )

# # # Loading from Gg cloud storage to Bigquery
# # # orders
# # load_orders_gcs_to_bq = GCSToBigQueryOperator(
# #     task_id='load_orders_to_bq',
# #     bucket='warehouse8',
# #     source_objects=['dataupdated/orders.csv'],  # Can be a list of files or a wildcard pattern
# #     destination_project_dataset_table='datawarehouse-423912:warehouse.orders',
# #     source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
# #     write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
# #     skip_leading_rows=1,  # Skip header row if CSV
# #     schema_fields=[
# #         {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
# #         {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
# #         {'name': 'employee_id', 'type': 'STRING', 'mode': 'NULLABLE'},
# #         {'name': 'order_date', 'type': 'DATE', 'mode': 'NULLABLE'},
# #         {'name': 'required_date', 'type': 'DATE', 'mode': 'NULLABLE'},
# #         {'name': 'shipped_date', 'type': 'DATE', 'mode': 'NULLABLE'},
# #         {'name': 'ship_via', 'type': 'INTEGER', 'mode': 'NULLABLE'},
# #         {'name': 'freight', 'type': 'STRING', 'mode': 'NULLABLE'},
# #         {'name': 'ship_name', 'type': 'STRING', 'mode': 'NULLABLE'},
# #         {'name': 'ship_address', 'type': 'STRING', 'mode': 'NULLABLE'},
# #         {'name': 'ship_region', 'type': 'STRING', 'mode': 'NULLABLE'},
# #         {'name': 'ship_postal_code', 'type': 'STRING', 'mode': 'NULLABLE'},
# #         {'name': 'ship_country', 'type': 'STRING', 'mode': 'NULLABLE'},
# #         # Add schema fields as per your data structure
# #     ],
# #     gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
# #     max_bad_records=10,  # Allow up to 10 bad records before failing
# #     field_delimiter=',',  # Specify the field delimiter
# #     ignore_unknown_values=True, 
# #     dag=dag  
# # )

# # load_orders_gcs_to_bq

# def read_sql_file(file_path):
#     with open(file_path, 'r') as file:
#         sql_content = file.read()
#         print(sql_content) # For debugging purposes
#         return sql_content

# UpdateCustomer=read_sql_file('/opt/airflow/dags/SQL_Queries/Update_Customer.sql')  
# DimDateQuery = read_sql_file('/opt/airflow/dags/SQL_Queries/DimDate.sql')
# DimOdersDetailsQuery=read_sql_file('/opt/airflow/dags/SQL_Queries/DimOrders_details.sql')
# DimCustomer = read_sql_file('/opt/airflow/dags/SQL_Queries/DimCustomer.sql')                                
# DimProduct = read_sql_file('/opt/airflow/dags/SQL_Queries/DimProduct.sql')
# FactTable = read_sql_file('/opt/airflow/dags/SQL_Queries/Fact_table.sql')
# RFMTable = read_sql_file('/opt/airflow/dags/SQL_Queries/RFM_table.sql')
# SaleTable = read_sql_file('/opt/airflow/dags/SQL_Queries/Sale_table.sql')
# OrderFactTable = read_sql_file('/opt/airflow/dags/SQL_Queries/Order_Fact_Table.sql')

# # Update Customer in Bigquerry

# update_customer= BigQueryInsertJobOperator(

#     task_id='update_customer',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query": UpdateCustomer,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )
# t1=update_customer


# # Task to extract and transform DimCustomer
# transform_dim_customer= BigQueryInsertJobOperator(

#     task_id='transform_dim_customer',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query": DimCustomer,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )
# t2=transform_dim_customer




# #Task to extract and transform DimDate
# transform_dim_date= BigQueryInsertJobOperator(

#     task_id='transform_dim_date',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query": DimDateQuery,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )

# t3= transform_dim_date


# #Task to extract and transform DimOrdersDetails
# transform_dim_orders_details= BigQueryInsertJobOperator(

#     task_id='transform_dim_orders_details',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query": DimOdersDetailsQuery,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )

# t4= transform_dim_orders_details

# #Task to extract and transform DimProduct
# transform_dim_product= BigQueryInsertJobOperator(

#     task_id='transform_dim_product',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query": DimProduct,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )

# t5 = transform_dim_product

# #Task to transform Fact Table
# transform_fact_table= BigQueryInsertJobOperator(

#     task_id='transform_fact_table',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query": FactTable,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )

# t6 = transform_fact_table

# #Create RFM
# create_FRM_table= BigQueryInsertJobOperator(

#     task_id='create_FRM_table',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query":RFMTable ,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )

# t7 = create_FRM_table


# #Create Sale Table 
# create_Sale_Table= BigQueryInsertJobOperator(

#     task_id='create_Sale_Table',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query":SaleTable ,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )

# t8 = create_Sale_Table

# #Transform Order Fact Table
# transform_Order_Fact_Table= BigQueryInsertJobOperator(

#     task_id='transform_Order_Fact_Table',
#     location='US',  # Change to your BigQuery dataset location
#     gcp_conn_id='google_cloud_default',
#     configuration = {
#         "query": {
#             "query":OrderFactTable ,
#             "useLegacySql": False,
#         }
#     },
#     dag=dag,
# )

# t9 = transform_Order_Fact_Table

# TaskDelay=BashOperator(
#     task_id="delay_bash_task", 
#     dag=dag,
#     bash_command="sleep 5s")



# # t0=load_orders_gcs_to_bq #Set task dependencies
# t1 >> [t2,t3,t4,t5] >> t6 >> t7 >> t8 >> t9









# # #train model
# # def model_training(): 
# #     train_model()

# # modelTraining=PythonOperator(
# #     task_id='train_model',
# #     python_callable=model_training, 
# #     dag=dag)



# # t8=modelTraining


# # def run_bigquery_sql():
# #     client = bigquery.Client()
# #     query=read_sql_file('/opt/airflow/dags/SQL_Queries/DimDate.sql')
# #     query_job=client.query(query) 
# #     results=query_job.result() # Wait for the job to complete.

# # run_sql_task=PythonOperator(
# #     task_id='run_bigquery_sql',
# #     python_callable=run_bigquery_sql, 
# #     dag=dag,)

# # t0 = run_sql_task #Set task dependencies

# # [t1, t2,13,14,15,16] >> t7 >> TaskDelay >> t8


