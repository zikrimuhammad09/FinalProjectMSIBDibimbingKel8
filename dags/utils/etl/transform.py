def remove_duplicates(df):
    print("Checking duplicates...")
    print("Before: ", df.shape)
    df.drop_duplicates(inplace=True)
    print("After: ", df.shape)

def remove_missing_values(df):
    print("Checking missing values...")
    print("Before: ", df.shape)
    df.dropna(inplace=True)
    print("After: ", df.shape)
    

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

    return "Sukses Transform Data"


