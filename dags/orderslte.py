from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# from clustering import train_model 


# os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/opt/airflow/config/gcloud_service_key.json' ## đã chỉnh về project củ mình, nhưng k hiểu

#ELT của anh Tuấn Thành

#DAG arguments 
default_args = {
    'owner': 'ngquynh',
    'depends_on-past': False,
    # 'start_date'= datetime(2024, 5, 27), 
    'email on failure': False,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=2),}

#DAG definition

dag = DAG(
    'oltp_to_olap_oders_transform',
    default_args=default_args,
    description='Transform OLTP to OLAP schema in BigQuery',
    template_searchpath='/opt/airflow/dags/SQL_Queries', ### không hiểu
    schedule_interval=None,
    start_date=datetime(2024, 5, 27),
)


load_orders_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_orders_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/orders.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.orders',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'employee_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'required_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'shipped_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'ship_via', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'freight', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ship_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ship_address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ship_region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ship_postal_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ship_country', 'type': 'STRING', 'mode': 'NULLABLE'},
        # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)



def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_content = file.read()
        print(sql_content) # For debugging purposes
        return sql_content

DimDateQuery = read_sql_file('/opt/airflow/dags/SQL_Queries/DimDate.sql')
# DimSalesPersonQuery=read_sql_file('/opt/airflow/dags/SQL_Queries/DimSalesPerson.sql')
# DimSalesReasonQuery = read_sql_file('/opt/airflow/dags/SQL_Queries/DimSalesPerson.sql')                                
# DimTerritoryQuery = read_sql_file('/opt/airflow/dags/SQL_Queries/DimTerritory.sql')
# DimCreditCardQuery=read_sql_file('/opt/airflow/dags/SQL_Queries/DimCreditCard.sql')
# DimCustomerQuery=read_sql_file('/opt/airflow/dags/SQL_Queries/DimCustomer.sql')
# FactSalesQuery=read_sql_file('/opt/airflow/dags/SQL_Queries/FactSales.sql')


#Task to extract and transform DimDate

load_dim_date= BigQueryInsertJobOperator(

    task_id='load_dim_date',
    location='US',  # Change to your BigQuery dataset location
    gcp_conn_id='google_cloud_default',
    configuration = {
        "query": {
            "query": DimDateQuery,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

load_orders_gcs_to_bq >> load_dim_date


# #Task to extract and transform DimSalesPerson


# load_dim_sales_person=BigQueryOperator(

#     task_id='load_dim_sales_person',
#     sql= DimSalesPersonQuery,
#     use_legacy_sql=False,
#     write_disposition='WRITE_TRUNCATE', #Options: WRITE TRUNCATE, WRITE APPEND, WRITE EMPTY
#     create_disposition='CREATE_IF_NEEDED', # Options: CREATE_IF NEEDED, CREATE NEVER
#     allow_large_results=True,
#     dag=dag,)

# t2=load_dim_sales_person

# #Task to extract and transform DimSalesReason

# load_dim_sales_reason=BigQueryOperator(
#     task_id='load_dim_sales_reason',
#     sql=DimSalesReasonQuery,
#     use_legacy_sql=False,
#     write_disposition='WRITE_TRUNCATE', #Options: WRITE TRUNCATE, WRITE APPEND, WRITE EMPTY 
#     create_disposition='CREATE_IF_NEEDED', #Options: CREATE IF NEEDED, CREATE NEVER
#     allow_large_results=True,
#     dag=dag,)

# t3=load_dim_sales_reason

# load_dim_territory=BigQueryOperator(
#     task_id='load dia territory',
#     sql=DimTerritoryQuery,
#     use_legacy_sql=False,
#     write_disposition='WRITE TRUNCATE', #Options: WRITE TRUNCATE, WRITE APPENA, VRETE EXPEN
#     create_disposition= 'CREATE_IF_NEEDED', #Options/ CREATE IF NEEDED, CREATE NEVEN
#     allow_large_results=True,
#     dag=dag,)

# t4=load_dim_territory

# #Task to extract and transform FactSales

# load_fact_sales=BigQueryOperator(
#     task_id='load_fact_sales',
#     sql=FactSalesQuery,
#     use_legacy_sql=False,
#     write_disposition='WRITE TRUNCATE', #Options: WRITE TRUNCATE, WRITE APPEND, RITE FHOTY
#     create_disposition='CREATE_IF_NEEDED', #Options: CREATE IF NEEDED, CREATE NEVER
#     allow_large_results=True,
#     dag=dag,)

# t7=load_fact_sales

# #Task to extract and transform DinCreditCard

# load_dim_credit_card=BigQueryOperator(

#     task_id="load_dim_credit_card", 
#     sql= DimCreditCardQuery,
#     use_legacy_sql=False,
#     write_disposition='WRITE TRUNCATE', #Options: WRITE TRUNCATE, WRITE APPEND, WRITE EMPIY
#     create_disposition='CREATE_IF_NEEDED', #Options: CREATE IF NEEDED, CREATE NEVER
#     allow_large_results=True,
#     dag=dag,)

# t5= load_dim_credit_card

# #Task to extract and, transform DisCustomer

# load_dim_customer=BigQueryOperator(

#     task_id='load_dim_customer', 
#     sql= DimCustomerQuery,
#     use_legacy_sql=False,
#     write_disposition='WRITE TRUNCATE', #Options: URITE TRUBICATE WRITE APPEND, WRITE BOPTY
#     create_disposition='CREATE_IF NEEDED',  #Options: CREATE IF NEEDED, CREATE NIVE
#     allow_large_results=True,
#     dag=dag,)

# t6=load_dim_customer

# #train model
# def model_training(): 
#     train_model()

# modelTraining=PythonOperator(
#     task_id='train_model',
#     python_callable=model_training, 
#     dag=dag)

# TaskDelay=BashOperator (
#     task_id="delay_bash_task", 
#     dag=dag,
#     bash_command="sleep 5s")

# t8=modelTraining

# TaskDelay = BashOperator(task_id='delay_bash_task',
#                          dag=dag,
#                          bash_command='sleep 5s')

# def run_bigquery_sql():
#     client = bigquery.Client()
#     query=read_sql_file('/opt/airflow/dags/SQL_Queries/DimDate.sql')
#     query_job=client.query(query) 
#     results=query_job.result() # Wait for the job to complete.

# run_sql_task=PythonOperator(
#     task_id='run_bigquery_sql',
#     python_callable=run_bigquery_sql, 
#     dag=dag,)

# t0 = run_sql_task #Set task dependencies

# [t1, t2,13,14,15,16] >> t7 >> TaskDelay >> t8


