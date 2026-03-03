import duckdb

conn=duckdb.connect("db/warehouse/music_warehouse.duckdb")


print(conn.execute("""
    SELECT * FROM event LIMIT 10;
""").fetchall())


conn.close()