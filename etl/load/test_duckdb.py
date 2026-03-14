import duckdb

conn=duckdb.connect("db/warehouse/music_warehouse.duckdb")


conn.execute("COPY event TO 'event.csv' (HEADER, DELIMITER ',')")

conn.close()