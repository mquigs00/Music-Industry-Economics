import duckdb
from config.paths import CREATE_TABLES_SQL, DROP_TABLES_SQL

def create_tables(conn):
    with open(CREATE_TABLES_SQL, "r") as f:
        sql = f.read()
    conn.execute(sql)

def drop_tables(conn):
    with open(DROP_TABLES_SQL, "r") as f:
        sql = f.read()
    conn.execute(sql)