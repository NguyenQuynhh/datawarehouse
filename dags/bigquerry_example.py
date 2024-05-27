from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'nquynh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to perform ETL transformation
def etl_transformation():
    # Extract data from Google Cloud Storage
    gcs_uri = 'gs://warehouse8/dataupdated/categories.csv'
    df = pd.read_csv(gcs_uri)



    # Perform transformation

    # # Example transformation: convert 'date' column to datetime format
    # df['date'] = pd.to_datetime(df['date'])

    # # Upload transformed data to BigQuery
    # project_id = 'your_project_id'
    # dataset_id = 'your_dataset_id'
    # table_id = 'your_table_id'
    # df.to_gbq(destination_table=f"{project_id}.{dataset_id}.{table_id}",
    #           project_id=project_id,
    #           if_exists='replace')


    # Loading data to Bigquerry

# Define DAG




with DAG(
    dag_id='bigquery_example',
    default_args=default_args,
    description='An example DAG to run a BigQuery SQL query',
    schedule_interval=None,  # Run on demand
    start_date=datetime(2024, 5, 26),
    catchup=False,
) as dag:

    # SQL query to run on BigQuery
    sql_query = """
        INSERT INTO `datawarehouse-423912.1.categories` 
        VALUES (2, "quynh")
            """

    # Task to execute the BigQuery SQL query

   
    run_bigquery_query = BigQueryInsertJobOperator(
        task_id='run_bigquery_query',
        configuration={
            "query": {
                "query": (
                    """ 
                        INSERT INTO `datawarehouse-423912.1.categories`
                        VALUES (2, "quynh")
                    """
                ),
                "useLegacySql": False,
            }
        },
        location='us'
    )
    run_bigquery_query
    




# with DAG('ETL_Process', default_args=default_args, schedule_interval=None) as dag:
#     # Task to extract data from Google Cloud Storage and load into BigQuery
#     etl_task = PythonOperator(
#         task_id='perform_etl',
#         python_callable=etl_transformation
#     )
#  # Define task dependencies
#     >> 
# if __name__ == "__main__":
#     dag.cli()
   

