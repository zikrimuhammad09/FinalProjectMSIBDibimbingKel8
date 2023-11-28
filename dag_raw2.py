from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import os
import glob
import pandas as pd
import psycopg2
import fastavro
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, TIMESTAMP, FLOAT, Text, Boolean, func
import logging


default_args = {
    "owner": "user",
    "depends_on_post": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=40),
}

# Initialize the engine globally
engine = None

def config():
    # Konfigurasi koneksi ke database PostgreSQL
    username = 'user'
    password = 'password'
    db_name = 'postgres_db'
    port = '5432'
    host = 'dataeng-ops-postgres'
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{db_name}'
    Variable.set('database_connection', connection_string)

def create_table(**context):
    global engine

    # Membuat objek MetaData
    metadata = MetaData()

    # Define the tables
    t_cust = Table(
        'customers',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('first_name', String),
        Column('last_name', String),
        Column('address', String),
        Column('gender', String),
        Column('zip_code', String),
        extend_existing=True,
        schema="public"
    )

    t_login = Table(
        'login_attempts_history',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('customer_id', Integer),
        Column('login_successful', Boolean),
        Column('attempted_at', TIMESTAMP, default=func.now()),
        extend_existing=True,
        schema="public"
    )

    t_product = Table(
        'products',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('price', FLOAT),
        Column('category_id', Integer),
        Column('supplier_id', Integer),
        extend_existing=True,
        schema="public"
    )

    t_product_cat = Table(
        'product_categories',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        extend_existing=True,
        schema="public"
    )

    t_supplier = Table(
        'suppliers',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('country', String),
        extend_existing=True,
        schema="public"
    )

    t_coupon = Table(
        'coupons',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('discount_percent', FLOAT),
        extend_existing=True,
        schema="public"
    )

    t_order = Table(
        'orders',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('customer_id', Integer),
        Column('status', Text),
        Column('created_at', TIMESTAMP, default=func.now()),
        extend_existing=True,
        schema="public"
    )

    t_order_item = Table(
        'order_items',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('order_id', Integer),
        Column('product_id', Integer),
        Column('amount', Integer),
        Column('coupon_id', Integer),
        extend_existing=True,
        schema="public"
    )

    # Membuat tabel di database
    connection_string = Variable.get('database_connection')
    engine = create_engine(connection_string)
    metadata.create_all(bind=engine)
    try:
        metadata.create_all(bind=engine)
        logging.info("Tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")

    # Commit the transaction
    engine.dispose()

def read_insert(**context):
    # context['ti'].xcom_pull(key='create_table')
    # Membaca file CSV
    customer = r'D:\fp_dibimbing\data'  # Path folder yang berisi file-file data
    if not os.path.exists(customer):
        print(f"Directory '{customer}' does not exist. Exiting read_insert task.")
        return

    # Continue with reading and inserting data
    all_csv_files = [file for file in os.listdir(customer) if file.endswith('.csv')]

    if not all_csv_files:
        print(f"No CSV files found in '{customer}'. Exiting read_insert task.")
        return
    # Membaca semua file CSV dalam folder
    all_csv_files = [file for file in os.listdir(customer) if file.endswith('.csv')]

    # Membaca setiap file CSV dan menggabungkannya menjadi satu DataFrame serta memasukan ke database
    cust = [pd.read_csv(os.path.join(customer, file)) for file in all_csv_files]
    cust_merg = pd.concat(cust, ignore_index=True)
    table_name = 'customers'
    cust_merg.to_sql(table_name, con=engine, if_exists='replace', index=False)

    # Membaca file JSON
    login_attempts = r'D:\fp_dibimbing\data'
    file_extension_json = 'login_attempts_*.json'
    all_files_json = glob.glob(os.path.join(login_attempts, file_extension_json))
    log_attempts = pd.concat((pd.read_json(file) for file in all_files_json), ignore_index=True)
    table_name = 'login_attempts_history'
    log_attempts.to_sql(table_name, con=engine, if_exists='replace', index=False)

    # Membaca file xls, json, parquet
    product = pd.read_excel(r'D:\fp_dibimbing\data\product.xls')
    table_name = 'products'
    product.to_sql(table_name, con=engine, if_exists='replace', index=False)

    product_category = pd.read_excel(r'D:\fp_dibimbing\data\product_category.xls')
    table_name = 'product_categories'
    product_category.to_sql(table_name, con=engine, if_exists='replace', index=False)

    supplier = pd.read_excel(r'D:\fp_dibimbing\data\supplier.xls')
    table_name = 'suppliers'
    supplier.to_sql(table_name, con=engine, if_exists='replace', index=False)

    coupons = pd.read_json(r'D:\fp_dibimbing\data\coupons.json')
    table_name = 'coupons'
    coupons.to_sql(table_name, con=engine, if_exists='replace', index=False)

    order = pd.read_parquet(r'D:\fp_dibimbing\data\order.parquet')
    table_name = 'orders'
    order.to_sql(table_name, con=engine, if_exists='replace', index=False)

    # Konfigurasi path file Avro
    avro_file_path = r'D:\fp_dibimbing\data\order_item.avro'
    table_name = 'order_items'  # Replace with the correct table name
    # Buka file Avro
    with engine.connect() as connection:
        with open(avro_file_path, "rb") as avro_file:
            avro_reader = fastavro.reader(avro_file)
            for record in avro_reader:
                connection.execute(table_name.insert().values(record))
        # Commit changes
        connection.commit()

    # # Menulis dataframe ke tabel di database
    # table_name = 'nama_tabel'  # Ganti dengan nama tabel yang diinginkan
    # cust.to_sql(table_name, con=engine, if_exists='replace', index=False)

    # Menutup koneksi
    engine.dispose()

dag3 = DAG(
    dag_id="dag3",
    default_args=default_args,
    start_date=datetime(2023, 11, 27),
    catchup=False,
    schedule=None
)

config_task = PythonOperator(
    task_id='config_task',
    python_callable=config,
    provide_context=True,
    dag=dag3
)

create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    provide_context=True,
    dag=dag3
)

read_insert_task = PythonOperator(
    task_id='read_insert_task',
    python_callable=read_insert,
    provide_context=True,
    dag=dag3
)

config_task >> create_table_task >> read_insert_task
