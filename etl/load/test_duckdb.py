import duckdb

conn=duckdb.connect("db/warehouse/music_warehouse.duckdb")


print(conn.execute("""
    SELECT * FROM event_ticket_price LIMIT 5;
""").fetchall())


conn.close()