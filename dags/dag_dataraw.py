from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, FLOAT, Text, Boolean, MetaData, Table
from sqlalchemy.sql import func
import os
import glob
import pandas as pd
import fastavro

# Konfigurasi koneksi ke database PostgreSQL
username = 'user'
password = 'password'
db_name = 'postgres_db'
port = '5432'
host = 'localhost'
# container_name = 'dataeng-ops-postgres'

# Membuat string koneksi
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')

# Membuat objek MetaData
metadata = MetaData()

# Membuat tabel
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
    Column('login_successfull', Boolean),
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
metadata.create_all(engine)

# Membaca file CSV
customer = r'D:\fp_dibimbing\data'  # Path folder yang berisi file-file data

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
