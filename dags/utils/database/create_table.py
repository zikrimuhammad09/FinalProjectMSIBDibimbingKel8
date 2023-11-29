from utils import database
import psycopg2

def create_table():
    # Konek ke Database
    conn = database.connection.conn()
    cursor = conn.cursor()
    try:
        # Buat tabel orders,coupons, customer, login_attempts, product_category, product, supplier, order_item
        cursor.execute('CREATE TABLE IF NOT EXISTS orders (id INTEGER, customer_id INTEGER, status TEXT, created_at TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS coupons (id INTEGER, order_id INTEGER, amount INTEGER, code TEXT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS customer (id INTEGER, name TEXT, email TEXT, address TEXT, phone TEXT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS login_attempts (id INTEGER, customer_id INTEGER, time TIMESTAMP, ip_address TEXT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS product_category (id INTEGER, name TEXT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS product (id INTEGER, name TEXT, price INTEGER, category_id INTEGER, supplier_id INTEGER)')
        cursor.execute('CREATE TABLE IF NOT EXISTS supplier (id INTEGER, name TEXT, address TEXT, phone TEXT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS order_item (id INTEGER, order_id INTEGER, product_id INTEGER, quantity INTEGER, price INTEGER)')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback() 
        cursor.close()
        return 1
    cursor.close()
    conn.close()

