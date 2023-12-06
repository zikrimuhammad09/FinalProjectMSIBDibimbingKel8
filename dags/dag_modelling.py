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

create_schema = PythonOperator(
    task_id         = "create_schema",
    python_callable = data_modelling.create_schema,
    provide_context = True,
    dag             = dag_modelling
)
create_dim_coupons = PythonOperator(
    task_id         = "create_dim_coupons",
    python_callable = data_modelling.create_dim_coupons,
    provide_context = True,
    dag             = dag_modelling
)
create_dim_customers = PythonOperator(
    task_id         = "create_dim_customers",
    python_callable = data_modelling.create_dim_customers,
    provide_context = True,
    dag             = dag_modelling
)
create_dim_date = PythonOperator(
    task_id         = "create_dim_date",
    python_callable = data_modelling.create_dim_date,
    provide_context = True,
    dag             = dag_modelling
)
create_dim_orders = PythonOperator(
    task_id         = "create_dim_orders",
    python_callable = data_modelling.create_dim_orders,
    provide_context = True,
    dag             = dag_modelling
)
create_dim_products = PythonOperator(
    task_id         = "create_dim_products",
    python_callable = data_modelling.create_dim_products,
    provide_context = True,
    dag             = dag_modelling
)
create_product_categories = PythonOperator(
    task_id         = "create_product_categories",
    python_callable = data_modelling.create_product_categories,
    provide_context = True,
    dag             = dag_modelling
)
create_suppliers = PythonOperator(
    task_id         = "create_suppliers",
    python_callable = data_modelling.create_suppliers,
    provide_context = True,
    dag             = dag_modelling
)
create_fact_login_attempt_history = PythonOperator(
    task_id         = "create_fact_login_attempt_history",
    python_callable = data_modelling.create_fact_login_attempt_history,
    provide_context = True,
    dag             = dag_modelling
)
create_fact_sales = PythonOperator(
    task_id         = "create_fact_sales",
    python_callable = data_modelling.create_fact_sales,
    provide_context = True,
    dag             = dag_modelling
)

create_schema >> [create_dim_coupons >> create_dim_customers >> create_dim_date >> create_dim_orders >> create_dim_products >> create_product_categories >> create_suppliers >> create_fact_login_attempt_history >> create_fact_sales]