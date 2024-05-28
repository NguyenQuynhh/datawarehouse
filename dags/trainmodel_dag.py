# train_model_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/path/to/your/dags/directory')

from train_model import train_model

# Default arguments
default_args = {
    'owner': 'ngquynh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'train_model_dag',
    default_args=default_args,
    description='A simple DAG to train a machine learning model',
    schedule_interval=timedelta(days=1),
)

# Define the task using PythonOperator
train_model_task = PythonOperator(
    task_id='train_model_task',
    python_callable=train_model,
    dag=dag,
)

train_model_task
