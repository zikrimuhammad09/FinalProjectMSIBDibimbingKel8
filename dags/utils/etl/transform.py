import pandas as pd

def remove_duplicates(df):
    print("Checking duplicates...")
    print("Before: ", df.shape)
    # set kolom id dari masing2 tabel menjadi index
    df.set_index('id')
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
        # set kolom id dari masing2 tabel menjadi index
        df.set_index('id')
        # Hapus missing value
        df.dropna(inplace=True)

def duplicate_data_customer(df_customer,df_order,df_login_attempts):
    print('Proses handling duplicate data customer')
    df_unique_customer = df_customer.drop_duplicates(keep='first')
    # find df_customer duplicates but except first
    df_duplicate_customer = df_customer[~df_customer.index.isin(df_unique_customer.index)]
    for idx in df_unique_customer.index:
        # find the row in df_customer that has the same each column in df_customer[idx] give new variable
        matching_rows = df_duplicate_customer[df_duplicate_customer.eq(df_customer.loc[idx]).all(axis=1)]
        # index matching_rows
        matching_rows_index = matching_rows.index
        # cari customer_id di df_order yang sama dengan matching_rows.index
        matching_order = df_order[df_order['customer_id'].isin(matching_rows_index)]
        # cari customer_id di df_login_attempts yang sama dengan matching_rows.index
        matching_login_attempts = df_login_attempts[df_login_attempts['customer_id'].isin(matching_rows_index)]
        # replace customer_id di df_order dengan idx
        matching_order['customer_id'] = idx
        # replace customer_id di df_login_attempts dengan idx
        matching_login_attempts['customer_id'] = idx

        # Update df_order with the modified values
        df_order.loc[matching_order.index] = matching_order

        # Update df_login_attempts with the modified values
        df_login_attempts.loc[matching_login_attempts.index] = matching_login_attempts

    df_customer = df_customer.drop_duplicates(keep='first')

    print('Handling Duplikat data Customer selesai')
    print(df_customer.head(20))
    print(df_order.head(50))
    print(df_login_attempts(50))
    return df_customer, df_order, df_login_attempts


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

    print("Transforming data...")

    # Tambahkan baris baru di coupons dengan index 0 dan isi kolomnya dengan 0
    df_coupons.loc[-1] = 0
    df_coupons.index += 1
    df_coupons.sort_index(inplace=True)

    # Drop customer_id 10000 dari df_login_attempts
    df_login_attempts.drop(df_login_attempts[df_login_attempts['customer_id'] == 10000].index, inplace=True)

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

    # handling duplicate data customer
    df_customer, df_order, df_login_attempts = duplicate_data_customer(df_customer, df_order, df_login_attempts)

    # Push Data ke XCom
    context['ti'].xcom_push(key='df_order', value=df_order)
    context['ti'].xcom_push(key='df_customer', value=df_customer)
    context['ti'].xcom_push(key='df_coupons', value=df_coupons)
    context['ti'].xcom_push(key='df_login_attempts', value=df_login_attempts)
    context['ti'].xcom_push(key='df_product_category', value=df_product_category)
    context['ti'].xcom_push(key='df_product', value=df_product)
    context['ti'].xcom_push(key='df_supplier', value=df_supplier)
    context['ti'].xcom_push(key='df_order_item', value=df_order_item)

    return "Sukses Transform Data"


