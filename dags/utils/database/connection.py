import psycopg2

def conn():
    conn = None
    config = {
        "user"      : "user",
        "password"  : "password",
        "database"  : "data_warehouse",
        "port"      : "5432",
        "host"      : "dataeng-warehouse-postgres"
    }   
    try:
        # Connect to PostgreSQL
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**config)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn
