import duckdb
from config.paths import LOCAL_DIM_ARTISTS_PATH, LOCAL_DIM_VENUES_PATH, LOCAL_DIM_PROMOTERS_PATH, LOCAL_DIM_CITIES_PATH, LOCAL_DIM_STATES_PATH, LOCAL_DIM_COUNTRIES_PATH, LOCAL_CURATED_EVENTS_GLOB_PATH

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

def load_facts(conn):
    conn.execute("DELETE FROM event_to_artist")
    conn.execute("DELETE FROM event_to_promoter")
    conn.execute("DELETE FROM event_ticket_price")
    conn.execute("DELETE FROM event")

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE staging_event AS
        SELECT *
        FROM read_csv_auto(?)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY signature ORDER BY 1) = 1
    """, [LOCAL_CURATED_EVENTS_GLOB_PATH])

    conn.execute("""
        INSERT OR IGNORE INTO event (weekly_rank, event_name, venue_id, start_date, end_date, dates, signature, gross_receipts_us, gross_receipts_canadian, attendance, capacity, num_shows, num_sellouts, schema_id, source_id, s3_uri)
        SELECT weekly_rank, event_name, venue_id, start_date, end_date, dates, signature, gross_receipts_us, gross_receipts_canadian, attendance, capacity, num_shows, num_sellouts, schema_id, source_id, s3_uri
        FROM staging_event
    """)

    conn.execute("""
        INSERT INTO event_to_artist (event_id, artist_id)
        SELECT DISTINCT
            event.id,
            UNNEST(
                string_split(
                    replace(replace(artist_ids, '[', ''), ']', ''),
                    ','
                )
            )::INTEGER
        FROM staging_event
        JOIN event on event.signature = staging_event.signature
        WHERE staging_event.artist_ids IS NOT NULL and staging_event.artist_ids != '[]'
    """)

    conn.execute("""
            INSERT INTO event_ticket_price (event_id, ticket_price)
            SELECT DISTINCT
                event.id,
                UNNEST(
                    string_split(
                        replace(replace(ticket_prices, '[', ''), ']', ''),
                        ','
                    )
                )::INTEGER
            FROM staging_event
            JOIN event on event.signature = staging_event.signature
            WHERE staging_event.ticket_prices IS NOT NULL and staging_event.ticket_prices != '[]'
        """)