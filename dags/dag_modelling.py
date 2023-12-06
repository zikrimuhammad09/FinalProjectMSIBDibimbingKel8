from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils import data_modelling

default_args = {
    "owner"            : "user",
    "depends_on_post"  : False,
    "retries"          : 2,
    "retry_delay"      : timedelta(seconds=40)
}

dag_modelling = DAG (
    dag_id        = "dag_modelling",
    default_args  = default_args,
    start_date    = datetime(2023, 11, 27),
    catchup       = False,
    schedule      = None
)

data_modelling = PythonOperator(
    task_id         = "data_modelling",
    python_callable = data_modelling.data_modelling,
    provide_context = True,
    dag             = dag_modelling
)

data_modelling
