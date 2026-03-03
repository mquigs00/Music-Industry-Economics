import duckdb
from create_schema import create_schema
from load_tables import load_dimension, load_facts
from pathlib import Path
from config.paths import DB_PATH

def main():
    conn = duckdb.connect(DB_PATH)

    #create_schema(conn)
    #load_dimension(conn)
    load_facts(conn)

    conn.close()

if __name__ == "__main__":
    main()