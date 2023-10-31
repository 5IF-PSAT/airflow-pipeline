CREATE TABLE IF NOT EXISTS staging_bus_delay_weekday_weekend_hour (
    id varchar(255) NOT NULL PRIMARY KEY,
    day_type varchar(255),
    hour integer,
    avg_delay double precision,
    min_delay double precision, 
    max_delay double precision, 
    count_delay integer,
    avg_gap double precision,
    min_gap double precision,
    max_gap double precision
);

CREATE TABLE IF NOT EXISTS staging_bus_delay_weekday_weekend_hour_location_incident (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    day_type varchar(255),
    hour integer,
    location varchar(255),
    incident varchar(255),
    avg_delay double precision,
    min_delay double precision, 
    max_delay double precision, 
    count_delay integer,
    avg_gap double precision,
    min_gap double precision,
    max_gap double precision
);