from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd

# Initialize default arguments for DAG
default_args = {
    "owner": "kel8",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# Define DAG
etl_dag = DAG(
    dag_id ="el_dag",
    default_args = default_args,
    start_date = datetime(2023,11,26),
    catchup = False,
)

# Extract Data From Parquet
def extract_data(**context):
    # import parquet file in ../data/order.parquet
    df = pd.read_parquet('data/order.parquet', engine='fastparquet')
    return context['ti'].xcom_push(key='df',value=df)
# Load Data to Postgres
def load_data(**context):
    final_df = context['ti'].xcom_pull(key='df')
    # Buat koneksi ke database
    hook = PostgresHook(postgres_conn_id='data_warehouse')
    # Check connection
    print(hook.get_conn())
    # final_df.to_sql('hands_on_final', hook.get_sqlalchemy_engine(), index=False, if_exists='replace')
    return 'Sukses Memasukkan Data'


# Define Task
extract_task = PythonOperator(
    task_id = 'extract_data',
    python_callable = extract_data,
    provide_context = True,
    dag = etl_dag
)

load_task = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data,
    provide_context = True,
    dag = etl_dag
)

# Define Dependencies
extract_task >> load_task