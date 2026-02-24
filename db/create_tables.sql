CREATE TABLE IF NOT EXISTS country (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    abbr VARCHAR (20) NOT NULL
);

CREATE TABLE IF NOT EXISTS state (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    abbr VARCHAR(20) NOT NULL,
    country_id INTEGER REFERENCES country(id)
);

CREATE TABLE IF NOT EXISTS city (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255),
    state_id INTEGER REFERENCES state(id)
);

CREATE TABLE IF NOT EXISTS venue (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    city_id INTEGER REFERENCES city(id),
    state_id INTEGER REFERENCES state(id)
);

CREATE TABLE IF NOT EXISTS special_event (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS promoter (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS artist (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    parent_id INT
);

CREATE TABLE IF NOT EXISTS event (
    id INTEGER PRIMARY KEY,
    signature VARCHAR(255) UNIQUE NOT NULL,
    weekly_rank INTEGER NOT NULL,
    venue_id INTEGER REFERENCES venue(id),
    event_name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    total_dates VARCHAR(255),
    gross_receipts_us INTEGER NOT NULL,
    gross_receipts_canadian INTEGER,
    attendance INTEGER,
    capacity INTEGER,
    num_shows INTEGER,
    num_sellouts INTEGER,
    special_event_id INTEGER,
    schema_id VARCHAR(255) NOT NULL,
    source_id INTEGER NOT NULL,
    s3_uri VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS event_to_promoter (
    event_id INTEGER REFERENCES event(id),
    promoter_id INTEGER REFERENCES promoter(id)
);

CREATE TABLE IF NOT EXISTS event_to_artist (
    event_id INTEGER REFERENCES event(id),
    artist_id INTEGER REFERENCES artist(id)
);