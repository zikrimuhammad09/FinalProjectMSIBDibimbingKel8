from utils import database

# Connect ke Database
conn = database.connection.conn()
def data_modelling():
    # Membuat objek kursor
    cursor = conn.cursor()

    # Membaca dan menjalankan isi file SQL
    sql_file = open('dags/utils/data_modelling/model.sql', 'r')
    cursor.execute(sql_file.read())

    # Commit perubahan (penting untuk perubahan yang mengubah data)
    conn.commit()

    # Menutup koneksi
    cursor.close()
    conn.close()

