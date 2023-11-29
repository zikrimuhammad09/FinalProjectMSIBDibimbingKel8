from utils import database
import psycopg2

def create_table():
    # Konek ke Database
    conn = database.connection.conn()
    cursor = conn.cursor()
    try:
        # Buat tabel orders, customers, coupons, login_attempt_history, product_categories, products, suppliers, order_items
        cursor.execute('CREATE TABLE IF NOT EXISTS orders (id INTEGER, customer_id INTEGER, status TEXT, created_at TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS coupons (id INTEGER, discount_percent FLOAT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS customers (id INTEGER, first_name VARCHAR(80), last_name VARCHAR(80), address VARCHAR(255), gender VARCHAR(20), zip_code VARCHAR(20))')
        cursor.execute('CREATE TABLE IF NOT EXISTS login_attempt_history (id INTEGER, customer_id INTEGER, login_successfull BOOLEAN, attempted_at TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS product_categories (id INTEGER, name VARCHAR(100))')
        cursor.execute('CREATE TABLE IF NOT EXISTS products (id INTEGER, name VARCHAR(100), price FLOAT, category_id INTEGER, supplier_id INTEGER)')
        cursor.execute('CREATE TABLE IF NOT EXISTS suppliers (id INTEGER, name VARCHAR(100), country VARCHAR(100))')
        cursor.execute('CREATE TABLE IF NOT EXISTS order_items (id INTEGER, order_id INTEGER, product_id INTEGER, amount INTEGER, coupon_id INTEGER)')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback() 
        cursor.close()
        return 1
    cursor.close()
    conn.close()

