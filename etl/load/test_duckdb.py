import duckdb

conn=duckdb.connect("db/warehouse/music_warehouse.duckdb")

'''
print(conn.execute("""
    SELECT * FROM artist LIMIT 10;
""").fetchall())
'''
conn.execute("DROP TABLE event_to_promoter")
conn.execute("DROP TABLE promoter")

conn.close()