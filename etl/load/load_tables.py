import duckdb
from config.paths import LOCAL_DIM_ARTISTS_PATH, LOCAL_DIM_VENUES_PATH, LOCAL_DIM_PROMOTERS_PATH, LOCAL_DIM_CITIES_PATH, LOCAL_DIM_STATES_PATH, LOCAL_DIM_COUNTRIES_PATH

def load_dimension(conn):
    conn.execute("DELETE FROM artist")
    conn.execute("DELETE FROM venue")
    conn.execute("DELETE FROM promoter")
    conn.execute("DELETE FROM city")
    conn.execute("DELETE FROM state")
    conn.execute("DELETE FROM country")

    conn.execute("""
        COPY country (id, name, slug, abbr)
        FROM ?
        (HEADER, DELIMITER ',')
    """, [LOCAL_DIM_COUNTRIES_PATH])

    conn.execute("""
        COPY artist (id, name, slug)
        FROM ?
        (HEADER, DELIMITER ',')
    """, [LOCAL_DIM_ARTISTS_PATH])

    conn.execute("""
        COPY state (id, name, slug, abbr, country_id)
        FROM ?
        (HEADER, DELIMITER ',')
    """, [LOCAL_DIM_STATES_PATH])

    conn.execute("""
        COPY city (id, name, slug, state_id)
        FROM ?
        (HEADER, DELIMITER ',')
    """, [LOCAL_DIM_CITIES_PATH])

    conn.execute("""
        COPY venue (id, name, slug, city_id, state_id)
        FROM ?
        (HEADER, DELIMITER ',')
    """, [LOCAL_DIM_VENUES_PATH])

    conn.execute("""
        COPY promoter(id, name, slug)
        FROM ?
        (HEADER, DELIMITER ',')
    """, [LOCAL_DIM_PROMOTERS_PATH])