from utils import database

# Connect ke Database
conn = database.connection.conn()
def create_schema():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/schema.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()
    
    # Menutup koneksi
    cursor.close()
    conn.close()

def create_dim_coupons():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/dim_coupons.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

def create_dim_customers():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/dim_customers.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

def create_dim_date():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/dim_date.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

def create_dim_orders():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/dim_orders.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

def create_dim_products():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/dim_products.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()
    
def create_product_categories():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/product_categories.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

def create_suppliers():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/suppliers.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

def create_fact_login_attempt_history():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/fact_login_attempt_history.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

def create_fact_sales():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/fact_sales.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

def create_fact_customer_conversion_rates():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/fact_customer_conversion_rates.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()
    
    # Menutup koneksi
    cursor.close()
    conn.close()

def create_fact_popular_products():
    # Membuat objek kursor
    cursor = conn.cursor()
    
    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/script_sql/fact_popular_products.sql', 'r')
    cursor.execute(sql_file.read())
    
    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()
    
    # Menutup koneksi
    cursor.close()
    conn.close()