CREATE ROLE etl_writer       LOGIN PASSWORD 'etl_writer';
CREATE ROLE dashboard_reader LOGIN PASSWORD 'dashboard_reader';

CREATE SCHEMA dw AUTHORIZATION etl_writer;
GRANT USAGE ON SCHEMA dw TO dashboard_reader;

SET ROLE etl_writer;
CREATE TABLE dw.live_quakes (
    event_id      TEXT PRIMARY KEY,
    event_time    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    mag           DOUBLE PRECISION,
    mag_type      TEXT,
    place         TEXT,
    region        TEXT,
    lon           DOUBLE PRECISION NOT NULL,
    lat           DOUBLE PRECISION NOT NULL,
    depth_km      DOUBLE PRECISION,
    event_type    TEXT,
    status        TEXT,
    tsunami       BOOLEAN,
    sig           INTEGER,
    alert         TEXT,
    url           TEXT,
    _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    _batch_id     BIGINT
);
CREATE INDEX idx_live_time ON dw.live_quakes (event_time DESC);
CREATE TABLE dw.dim_date (
    date_key   INTEGER PRIMARY KEY,
    full_date  DATE NOT NULL UNIQUE,
    year       SMALLINT NOT NULL,
    quarter    SMALLINT NOT NULL,
    month      SMALLINT NOT NULL,
    day        SMALLINT NOT NULL,
    weekday    SMALLINT NOT NULL
);

CREATE TABLE dw.dim_region (
    region_key  INTEGER PRIMARY KEY,
    region      TEXT NOT NULL UNIQUE
);

CREATE TABLE dw.dim_magnitude (
    mag_class_key  SMALLINT PRIMARY KEY,
    label          TEXT NOT NULL,
    min_mag        DOUBLE PRECISION NOT NULL,
    max_mag        DOUBLE PRECISION NOT NULL
);

CREATE TABLE dw.dim_depth (
    depth_class_key  SMALLINT PRIMARY KEY,
    label            TEXT NOT NULL,
    min_km           DOUBLE PRECISION NOT NULL,
    max_km           DOUBLE PRECISION NOT NULL
);

CREATE TABLE dw.fact_earthquake (
    event_id         TEXT PRIMARY KEY,
    date_key         INTEGER  NOT NULL REFERENCES dw.dim_date(date_key),
    region_key       INTEGER  NOT NULL REFERENCES dw.dim_region(region_key),
    mag_class_key    SMALLINT NOT NULL REFERENCES dw.dim_magnitude(mag_class_key),
    depth_class_key  SMALLINT NOT NULL REFERENCES dw.dim_depth(depth_class_key),
    event_time       TIMESTAMPTZ NOT NULL,
    mag              DOUBLE PRECISION NOT NULL,
    depth_km         DOUBLE PRECISION NOT NULL,
    lon              DOUBLE PRECISION NOT NULL,
    lat              DOUBLE PRECISION NOT NULL,
    sig              INTEGER,
    felt             INTEGER,
    tsunami          BOOLEAN,
    alert            TEXT
);
CREATE INDEX idx_fact_date   ON dw.fact_earthquake (date_key);
CREATE INDEX idx_fact_region ON dw.fact_earthquake (region_key);
CREATE INDEX idx_fact_mag    ON dw.fact_earthquake (mag);

CREATE TABLE dw.agg_region_month (
    region     TEXT     NOT NULL,
    year       SMALLINT NOT NULL,
    month      SMALLINT NOT NULL,
    n_quakes   INTEGER  NOT NULL,
    avg_mag    DOUBLE PRECISION,
    max_mag    DOUBLE PRECISION,
    PRIMARY KEY (region, year, month)
);

CREATE TABLE dw.agg_mag_distribution (
    mag_bin    DOUBLE PRECISION PRIMARY KEY,
    n_quakes   BIGINT NOT NULL
);

RESET ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA dw TO dashboard_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_writer IN SCHEMA dw GRANT SELECT ON TABLES TO dashboard_reader;
