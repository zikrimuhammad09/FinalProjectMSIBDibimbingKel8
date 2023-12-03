from utils import database
import psycopg2

def create_table():
    # Konek ke Database
    conn = database.connection.conn()
    cursor = conn.cursor()
    try:
        # Buat tabel orders, customers, coupons, login_attempt_history, product_categories, products, suppliers, order_items
        cursor.execute('CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, created_at TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS coupons (id INTEGER PRIMARY KEY, discount_percent FLOAT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS customers (id INTEGER PRIMARY KEY, first_name VARCHAR(80), last_name VARCHAR(80), address VARCHAR(255), gender VARCHAR(20), zip_code VARCHAR(20))')
        cursor.execute('CREATE TABLE IF NOT EXISTS login_attempt_history (id INTEGER PRIMARY KEY, customer_id INTEGER, login_successfull BOOLEAN, attempted_at TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS product_categories (id INTEGER PRIMARY KEY, name VARCHAR(100))')
        cursor.execute('CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name VARCHAR(100), price FLOAT, category_id INTEGER, supplier_id INTEGER)')
        cursor.execute('CREATE TABLE IF NOT EXISTS suppliers (id INTEGER PRIMARY KEY, name VARCHAR(100), country VARCHAR(100))')
        cursor.execute('CREATE TABLE IF NOT EXISTS order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, amount INTEGER, coupon_id INTEGER)')

        # Buat relasi antar tabel
        cursor.execute('ALTER TABLE orders ADD FOREIGN KEY (customer_id) REFERENCES customers (id)')
        cursor.execute('ALTER TABLE login_attempt_history ADD FOREIGN KEY (customer_id) REFERENCES customers (id)')
        cursor.execute('ALTER TABLE products ADD FOREIGN KEY (category_id) REFERENCES product_categories (id)')
        cursor.execute('ALTER TABLE products ADD FOREIGN KEY (supplier_id) REFERENCES suppliers (id)')
        cursor.execute('ALTER TABLE order_items ADD FOREIGN KEY (order_id) REFERENCES orders (id)')
        cursor.execute('ALTER TABLE order_items ADD FOREIGN KEY (product_id) REFERENCES products (id)')
        cursor.execute('ALTER TABLE order_items ADD FOREIGN KEY (coupon_id) REFERENCES coupons (id)')

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback() 
        cursor.close()
        return 1
    cursor.close()
    conn.close()

