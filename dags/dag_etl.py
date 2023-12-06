from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import utils
from utils import etl

default_args = {
    "owner"            : "user",
    "depends_on_post"  : False,
    "retries"          : 2,
    "retry_delay"      : timedelta(seconds=40)
}

dag_etl = DAG (
    dag_id        = "dag_etl",
    default_args  = default_args,
    start_date    = datetime(2023, 11, 27),
    catchup       = False,
    schedule      = None
)

create_table = PythonOperator(
    task_id         = "create_table",
    python_callable = utils.database.create_table,
    provide_context = True,
    dag             = dag_etl
)

extract_order = PythonOperator(
    task_id         = "extract_order",
    python_callable = etl.extract_order,
    provide_context = True,
    dag             = dag_etl
)

extract_customer = PythonOperator(
    task_id         = "extract_customer",
    python_callable = etl.extract_customer,
    provide_context = True,
    dag             = dag_etl
)

extract_coupons = PythonOperator(
    task_id         = "extract_coupons",
    python_callable = etl.extract_coupons,
    provide_context = True,
    dag             = dag_etl
)

extract_login_attempts = PythonOperator(
    task_id         = "extract_login_attempts",
    python_callable = etl.extract_login_attempts,
    provide_context = True,
    dag             = dag_etl
)

extract_product_category = PythonOperator(
    task_id         = "extract_product_category",
    python_callable = etl.extract_product_category,
    provide_context = True,
    dag             = dag_etl
)

extract_product = PythonOperator(
    task_id         = "extract_product",
    python_callable = etl.extract_product,
    provide_context = True,
    dag             = dag_etl
)

extract_supplier = PythonOperator(
    task_id         = "extract_supplier",
    python_callable = etl.extract_supplier,
    provide_context = True,
    dag             = dag_etl
)

extract_order_item = PythonOperator(
    task_id         = "extract_order_item",
    python_callable = etl.extract_order_item,
    provide_context = True,
    dag             = dag_etl
)

# extract_zip_code = PythonOperator(
#     task_id         = "extract_zip_code",
#     python_callable = etl.extract_zip_code,
#     provide_context = True,
#     dag             = dag_etl
# )

transform_task = PythonOperator(
    task_id         = "transform_data",
    python_callable = etl.transform_data,
    provide_context = True,
    dag             = dag_etl
)

load_task = PythonOperator(
    task_id         = "load_data",
    python_callable = etl.load_data,
    provide_context = True,
    dag             = dag_etl
)

create_table >> [extract_order, extract_customer, extract_coupons, extract_login_attempts, extract_product_category, extract_product, extract_supplier, extract_order_item] >> transform_task >> load_task