import glob
import pandas as pd
import fastavro
# Extract data order dari file parquet
def extract_order(**context):
    df = pd.read_parquet('data/order.parquet', engine='fastparquet')
    print(df.head())
    return context['ti'].xcom_push(key='df_order',value=df)

# Extract data customer dari file csv
def extract_customer(**context):
    list_of_csv_files = glob.glob('data/customer_*.csv')
    list_of_csv_files.sort()

    df = pd.concat(map(pd.read_csv, list_of_csv_files), ignore_index=True)
    print(df.head())
    return context['ti'].xcom_push(key='df_customer',value=df)

# Extract data coupons dari file json
def extract_coupons(**context):
    df = pd.read_json('data/coupons.json')
    print(df.head())
    return context['ti'].xcom_push(key='df_coupons',value=df)

# Extract data login_attempts dari file json
def extract_login_attempts(**context):
    list_of_json_files = glob.glob('data/login_attempts_*.json')
    list_of_json_files.sort()

    df = pd.concat(map(pd.read_json, list_of_json_files), ignore_index=True)
    print(df.head())
    return context['ti'].xcom_push(key='df_login_attempts',value=df)

# Extract data product_category dari file xls
def extract_product_category(**context):
    df = pd.read_excel('data/product_category.xls', index_col=0)
    print(df.head())
    return context['ti'].xcom_push(key='df_product_category',value=df)

# Extract data product dari file xls
def extract_product(**context):
    df = pd.read_excel('data/product.xls', index_col=0)
    print(df.head())
    return context['ti'].xcom_push(key='df_product',value=df)

# Extract data supplier dari file xls
def extract_supplier(**context):
    df = pd.read_excel('data/supplier.xls', index_col=0)
    print(df.head())
    return context['ti'].xcom_push(key='df_supplier',value=df)

# Extract data order_item dari file avro
def extract_order_item(**context):
    with open('data/order_item.avro', 'rb') as fo:
        avro_reader = fastavro.reader(fo)
        df = pd.DataFrame.from_records(avro_reader)
    print(df.head())
    return context['ti'].xcom_push(key='df_order_item',value=df)
    