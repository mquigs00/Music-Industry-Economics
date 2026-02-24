import duckdb
from config.paths import CREATE_TABLES_SQL

def create_schema(conn):
    with open(CREATE_TABLES_SQL, "r") as f:
        sql = f.read()
    conn.execute(sql)