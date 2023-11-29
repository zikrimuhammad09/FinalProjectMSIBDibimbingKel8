from utils import database
import psycopg2
import psycopg2.extras as extras
import numpy as np

# Connect ke Database
conn = database.connection.conn()

def execute_values(df, table):
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

def load_data(**context):
    # Ambil data dari XCom
    df_order = context['ti'].xcom_pull(key='df_order')
    df_customer = context['ti'].xcom_pull(key='df_customer')
    df_coupons = context['ti'].xcom_pull(key='df_coupons')
    df_login_attempts = context['ti'].xcom_pull(key='df_login_attempts')
    df_product_category = context['ti'].xcom_pull(key='df_product_category')
    df_product = context['ti'].xcom_pull(key='df_product')
    df_supplier = context['ti'].xcom_pull(key='df_supplier')
    df_order_item = context['ti'].xcom_pull(key='df_order_item')
    print(df_order.head())
    print(df_customer.head())
    print(df_coupons.head())
    print(df_login_attempts.head())
    print(df_product_category.head())
    print(df_product.head())
    print(df_supplier.head())
    print(df_order_item.head())

    # Insert data ke tabel orders
    execute_values(df_order, 'orders')
    # Insert data ke tabel customer
    execute_values(df_customer, 'customer')
    # Insert data ke tabel coupons
    execute_values(df_coupons, 'coupons')
    # Insert data ke tabel login_attempts
    execute_values(df_login_attempts, 'login_attempts')
    # Insert data ke tabel product_category
    execute_values(df_product_category, 'product_category')
    # Insert data ke tabel product
    execute_values(df_product, 'product')
    # Insert data ke tabel supplier
    execute_values(df_supplier, 'supplier')
    # Insert data ke tabel order_item
    execute_values(df_order_item, 'order_item')

    conn.close()

    return "Sukses Memasukkan Data"