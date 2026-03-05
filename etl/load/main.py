import duckdb
from database_scripts import create_tables, drop_tables
from load_tables import load_dimension, load_facts
from pathlib import Path
from config.paths import DB_PATH

def main():
    conn = duckdb.connect(DB_PATH)

    drop_tables(conn)
    create_tables(conn)
    load_dimension(conn)
    load_facts(conn)

    conn.close()

if __name__ == "__main__":
    main()