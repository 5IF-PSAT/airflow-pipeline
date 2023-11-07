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
    day_type varchar(255) NOT NULL,
    hour integer,
    location varchar(255),
    incident varchar(255),
    avg_delay double precision,
    min_delay double precision, 
    max_delay double precision, 
    count_delay integer,
    avg_gap double precision,
    min_gap double precision,
    max_gap double precision,
    count_gap integer
) PARTITION BY LIST (day_type);

CREATE TABLE IF NOT EXISTS staging_bus_delay_weekday PARTITION OF 
staging_bus_delay_weekday_weekend_hour_location_incident FOR VALUES IN ('weekday');

CREATE TABLE IF NOT EXISTS staging_bus_delay_weekend PARTITION OF
staging_bus_delay_weekday_weekend_hour_location_incident FOR VALUES IN ('weekend');

CREATE INDEX IF NOT EXISTS staging_bus_delay_weekday_weekend_hour_location_incident_idx 
ON staging_bus_delay_weekday_weekend_hour_location_incident (day_type);