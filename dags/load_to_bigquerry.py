from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash_operator import BashOperator

# from clustering import train_model 


# os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/opt/airflow/config/gcloud_service_key.json' ## đã chỉnh về project củ mình, nhưng k hiểu



#DAG arguments 
default_args = {
    'owner': 'ngquynh',
    'depends_on-past': False,
    'email on failure': False,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=2),}

#DAG definition

dag = DAG(
    'loading_to_bigquerry',
    default_args=default_args,
    description='Transform OLTP to OLAP schema in BigQuery',
    # template_searchpath='/opt/airflow/dags/SQL_Queries', ### không hiểu
    schedule_interval=None,
    start_date=datetime(2024, 5, 28),
)

# Task to load data category from GCS to BigQuery 
# category
load_categories_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_categories_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/categories.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.categories',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'categoryID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'categoryName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=';',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)


t1= load_categories_gcs_to_bq


# Loading data orders from Gg cloud storage to Bigquery
# orders
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

t2= load_orders_gcs_to_bq


# Loading data customers from Gg cloud storage to Bigquery

load_customers_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_customers_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/customers.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.customers',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'customerId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'companyName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contactName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contactTitle', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'postalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'fax', 'type': 'STRING', 'mode': 'NULLABLE'},
        
        # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)

t3= load_customers_gcs_to_bq


# Loading data employees from Gg cloud storage to Bigquery

load_employees_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_employees_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/employees.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.employees',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'employeeID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'lastName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'firstName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'titleOfCourtesy', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'birthDate', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'hireDate', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'postalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'homePhone', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'extension', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'photo', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'notes', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'reportsTo', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'photoPath', 'type': 'STRING', 'mode': 'NULLABLE'},
          # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)
t4= load_employees_gcs_to_bq

# Loading data employeeterritories from Gg cloud storage to Bigquery
load_employeeterritories_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_employeeterritories_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/employeeterritories.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.employeeterritories',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'employeeID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'territoryID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        
          # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)
t5= load_employeeterritories_gcs_to_bq



# Loading data orderdetails from Gg cloud storage to Bigquery
load_orderdetails_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_orderdetails_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/orderdetails.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.orderdetails',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'orderID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'productID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'unitPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'discount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    
          # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)
t6= load_orderdetails_gcs_to_bq



# Loading data products from Gg cloud storage to Bigquery
load_products_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_products_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/products.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.products',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'productID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'productName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'supplierID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'categoryID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'quantityPerUnit', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'unitPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'unitsInStock', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'unitsOnOrder', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'reorderLevel', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'discontinued', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        
          # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)
t7= load_products_gcs_to_bq


# Loading data regions from Gg cloud storage to Bigquery
load_regions_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_regions_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/regions.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.regions',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'regionID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'regionDescription', 'type': 'STRING', 'mode': 'NULLABLE'},
    
          # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)
t8= load_regions_gcs_to_bq

# Loading data shippers from Gg cloud storage to Bigquery
load_shippers_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_shippers_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/shippers.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.shippers',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'shipperID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'companyName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
    
          # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)
t9= load_shippers_gcs_to_bq


# Loading data suppliers from Gg cloud storage to Bigquery
load_suppliers_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_suppliers_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/suppliers.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.suppliers',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'supplierID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'companyName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contactName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contactTitle', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'postalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'fax', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'homePage', 'type': 'STRING', 'mode': 'NULLABLE'},
    
          # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)
t10= load_suppliers_gcs_to_bq


# Loading data territories from Gg cloud storage to Bigquery
load_territories_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_territories_to_bq',
    bucket='warehouse8',
    source_objects=['dataupdated/territories.csv'],  # Can be a list of files or a wildcard pattern
    destination_project_dataset_table='datawarehouse-423912:warehouse.territories',
    source_format='CSV',  # Adjust based on your file format (CSV, NEWLINE_DELIMITED_JSON, PARQUET, etc.)
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    skip_leading_rows=1,  # Skip header row if CSV
    schema_fields=[
        {'name': 'territoryID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'territoryDescription', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'regionID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    
          # Add schema fields as per your data structure
    ],
    gcp_conn_id='google_cloud_default',  # Change to your GCP connection ID
    max_bad_records=10,  # Allow up to 10 bad records before failing
    field_delimiter=',',  # Specify the field delimiter
    ignore_unknown_values=True, 
    dag=dag  
)
t11= load_territories_gcs_to_bq


def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_content = file.read()
        print(sql_content) # For debugging purposes
        return sql_content

UpdateCustomer=read_sql_file('/opt/airflow/dags/SQL_Queries/Update_Customer.sql')  
DimDateQuery = read_sql_file('/opt/airflow/dags/SQL_Queries/DimDate.sql')
DimOdersDetailsQuery=read_sql_file('/opt/airflow/dags/SQL_Queries/DimOrders_details.sql')
DimCustomer = read_sql_file('/opt/airflow/dags/SQL_Queries/DimCustomer.sql')                                
DimProduct = read_sql_file('/opt/airflow/dags/SQL_Queries/DimProduct.sql')
FactTable = read_sql_file('/opt/airflow/dags/SQL_Queries/Fact_table.sql')
RFMTable = read_sql_file('/opt/airflow/dags/SQL_Queries/RFM_table.sql')
SaleTable = read_sql_file('/opt/airflow/dags/SQL_Queries/Sale_table.sql')
OrderFactTable = read_sql_file('/opt/airflow/dags/SQL_Queries/Order_Fact_Table.sql')

# Update Customer in Bigquerry

# Task to extract and transform DimCustomer
transform_dim_customer= BigQueryInsertJobOperator(

    task_id='transform_dim_customer',
    location='US',  # Change to your BigQuery dataset location
    gcp_conn_id='google_cloud_default',
    configuration = {
        "query": {
            "query": DimCustomer,
            "useLegacySql": False,
        }
    },
    dag=dag,
)
t12=transform_dim_customer


#Task to extract and transform DimDate
transform_dim_date= BigQueryInsertJobOperator(

    task_id='transform_dim_date',
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

t13= transform_dim_date


#Task to extract and transform DimOrdersDetails
transform_dim_orders_details= BigQueryInsertJobOperator(

    task_id='transform_dim_orders_details',
    location='US',  # Change to your BigQuery dataset location
    gcp_conn_id='google_cloud_default',
    configuration = {
        "query": {
            "query": DimOdersDetailsQuery,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

t14= transform_dim_orders_details

#Task to extract and transform DimProduct
transform_dim_product= BigQueryInsertJobOperator(

    task_id='transform_dim_product',
    location='US',  # Change to your BigQuery dataset location
    gcp_conn_id='google_cloud_default',
    configuration = {
        "query": {
            "query": DimProduct,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

t15 = transform_dim_product

#Task to transform Fact Table
transform_fact_table= BigQueryInsertJobOperator(

    task_id='transform_fact_table',
    location='US',  # Change to your BigQuery dataset location
    gcp_conn_id='google_cloud_default',
    configuration = {
        "query": {
            "query": FactTable,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

t16 = transform_fact_table

#Create RFM
create_FRM_table= BigQueryInsertJobOperator(

    task_id='create_FRM_table',
    location='US',  # Change to your BigQuery dataset location
    gcp_conn_id='google_cloud_default',
    configuration = {
        "query": {
            "query":RFMTable ,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

t17 = create_FRM_table


#Create Sale Table 
create_Sale_Table= BigQueryInsertJobOperator(

    task_id='create_Sale_Table',
    location='US',  # Change to your BigQuery dataset location
    gcp_conn_id='google_cloud_default',
    configuration = {
        "query": {
            "query":SaleTable ,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

t18 = create_Sale_Table

#Transform Order Fact Table
transform_Order_Fact_Table= BigQueryInsertJobOperator(

    task_id='transform_Order_Fact_Table',
    location='US',  # Change to your BigQuery dataset location
    gcp_conn_id='google_cloud_default',
    configuration = {
        "query": {
            "query":OrderFactTable ,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

t19 = transform_Order_Fact_Table



TaskDelay=BashOperator(
    task_id="delay_bash_task", 
    dag=dag,
    bash_command="sleep 5s")

# # t0=load_orders_gcs_to_bq #Set task dependencies
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> TaskDelay >> [t12,t13,t14,t15] >> t16 >> t17 >> t18 >> t19

 
