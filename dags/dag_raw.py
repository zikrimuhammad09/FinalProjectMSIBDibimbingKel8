from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import os
import glob
import pandas as pd
import psycopg2
import fastavro

default_args = {
    "owner": "user",
    "depends_on_post": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=40),
}

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
    # Membuat objek MetaData
    metadata = {
        't_cust': {
            'name': 'customers',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'first_name', 'type': 'VARCHAR'},
                {'name': 'last_name', 'type': 'VARCHAR'},
                {'name': 'address', 'type': 'VARCHAR'},
                {'name': 'gender', 'type': 'VARCHAR'},
                {'name': 'zip_code', 'type': 'VARCHAR'}
            ]
        },
        't_login': {
            'name': 'login_attempts_history',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'customer_id', 'type': 'INTEGER'},
                {'name': 'login_successful', 'type': 'BOOLEAN'},
                {'name': 'attempted_at', 'type': 'TIMESTAMP', 'default': 'func.now()'}
            ]
        },
        't_product': {
            'name': 'products',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'name', 'type': 'VARCHAR'},
                {'name': 'price', 'type': 'FLOAT'},
                {'name': 'category_id', 'type': 'INTEGER'},
                {'name': 'supplier_id', 'type': 'INTEGER'}
            ]
        },
        't_product_cat': {
            'name': 'product_categories',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'name', 'type': 'VARCHAR'}
            ]
        },
        't_supplier': {
            'name': 'suppliers',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'name', 'type': 'VARCHAR'},
                {'name': 'country', 'type': 'VARCHAR'}
            ]
        },
        't_coupon': {
            'name': 'coupons',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'discount_percent', 'type': 'FLOAT'}
            ]
        },
        't_order': {
            'name': 'orders',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'customer_id', 'type': 'INTEGER'},
                {'name': 'status', 'type': 'TEXT'},
                {'name': 'created_at', 'type': 'TIMESTAMP', 'default': 'func.now()'}
            ]
        },
        't_order_item': {
            'name': 'order_items',
            'columns': [
                {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                {'name': 'order_id', 'type': 'INTEGER'},
                {'name': 'product_id', 'type': 'INTEGER'},
                {'name': 'amount', 'type': 'INTEGER'},
                {'name': 'coupon_id', 'type': 'INTEGER'}
            ]
        }
    }

    connection_string = Variable.get('database_connection')
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            try:
                for table_name, table_info in metadata.items():
                    create_table_query = f"CREATE TABLE {table_info['name']} ("
                    for column in table_info['columns']:
                        create_table_query += f"{column['name']} {column['type']}, "
                    create_table_query = create_table_query[:-2] + ");"
                    cursor.execute(create_table_query)

                # Commit the transaction
                conn.commit()
            except Exception as e:
                # Roll back the transaction in case of an exception
                conn.rollback()
                print(f"Error: {e}")

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
    table_name = t_order_item
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

dag2 = DAG(
    dag_id="dag2",
    default_args=default_args,
    start_date=datetime(2023, 11, 27),
    catchup=False,
    schedule=None
)

config_task = PythonOperator(
    task_id='config_task',
    python_callable=config,
    provide_context=True,
    dag=dag2
)

create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    provide_context=True,
    dag=dag2
)

read_insert_task = PythonOperator(
    task_id='read_insert_task',
    python_callable=read_insert,
    provide_context=True,
    dag=dag2
)

config_task >> create_table_task >> read_insert_task
