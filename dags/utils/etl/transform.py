import pandas as pd

def remove_duplicates(df):
    print("Checking duplicates...")
    print("Before: ", df.shape)
    df.drop_duplicates(inplace=True)    
    print("After: ", df.shape)

def remove_missing_values(df):
    print("Checking missing values...")
    is_null = df.isnull().sum()
    print(f'Jumlah missing values:\n{is_null}')

    if 'coupon_id' in df.columns:
        # Ubah kolom coupon_id di df_order_item yang missing value menjadi 0
        df['coupon_id'].fillna(0, inplace=True)
    elif is_null.sum() > 0:
        # Hapus missing value
        df.dropna(inplace=True)

def transform_data(**context):
    # Pull Data dari XCom
    df_order = context['ti'].xcom_pull(key='df_order')
    df_customer = context['ti'].xcom_pull(key='df_customer')
    df_coupons = context['ti'].xcom_pull(key='df_coupons')
    df_login_attempts = context['ti'].xcom_pull(key='df_login_attempts')
    df_product_category = context['ti'].xcom_pull(key='df_product_category')
    df_product = context['ti'].xcom_pull(key='df_product')
    df_supplier = context['ti'].xcom_pull(key='df_supplier')
    df_order_item = context['ti'].xcom_pull(key='df_order_item')
    # df_zip_code = context['ti'].xcom_pull(key='df_zip_code')

    print("Transforming data...")

    # Tambahkan baris baru di coupons dengan index 0 dan isi kolomnya dengan 0
    df_coupons.loc[-1] = 0
    df_coupons.index += 1
    df_coupons.sort_index(inplace=True)

    # Drop customer_id 10000 dari df_login_attempts
    df_login_attempts.drop(df_login_attempts[df_login_attempts['customer_id'] == 10000].index, inplace=True)

    # Ambil kolom zip, lat , lng, city, state_id, dan state_name dari df_zip_code
    # df_zip_code = df_zip_code[['zip', 'lat', 'lng', 'city', 'state_id', 'state_name']]

    # remove kolom pertama dari df_customer
    df_customer.drop(df_customer.columns[0], axis=1, inplace=True)

    # ubah tipe kolom discount_percent di df_coupons menjadi float
    df_coupons['discount_percent'] = df_coupons['discount_percent'].astype(float)

    # ubah kolom remove_successful menjadi login_successfull di df_login_attempts
    df_login_attempts.rename(columns={'login_successful': 'login_successfull'}, inplace=True)

    # remove data yang duplikat, missing value
    for df in [df_order, df_customer, df_coupons, df_login_attempts, df_product_category, df_product, df_supplier, df_order_item]:
        remove_duplicates(df)
        remove_missing_values(df)

    # Push Data ke XCom
    context['ti'].xcom_push(key='df_order', value=df_order)
    context['ti'].xcom_push(key='df_customer', value=df_customer)
    context['ti'].xcom_push(key='df_coupons', value=df_coupons)
    context['ti'].xcom_push(key='df_login_attempts', value=df_login_attempts)
    context['ti'].xcom_push(key='df_product_category', value=df_product_category)
    context['ti'].xcom_push(key='df_product', value=df_product)
    context['ti'].xcom_push(key='df_supplier', value=df_supplier)
    context['ti'].xcom_push(key='df_order_item', value=df_order_item)
    # context['ti'].xcom_push(key='df_zip_code', value=df_zip_code)

    return "Sukses Transform Data"


