import duckdb
from create_schema import create_schema
from load_tables import load_dimension
from pathlib import Path
from config.paths import DB_PATH

def main():
    conn = duckdb.connect(DB_PATH)

    #create_schema(conn)
    load_dimension(conn)

    conn.close()

if __name__ == "__main__":
    main()