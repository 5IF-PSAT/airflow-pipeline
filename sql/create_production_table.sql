CREATE TABLE IF NOT EXISTS production_weekday_weekend_hour (
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
    avg_temperature double precision,
    min_temperature double precision,
    max_temperature double precision,
    avg_humidity double precision,
    avg_rain double precision,
    max_rain double precision,
    min_rain double precision,
    avg_wind_speed double precision,
    max_wind_speed double precision,
    min_wind_speed double precision
);

SELECT a.day_type, a.hour, a.avg_temperature, a.min_temperature, 
a.max_temperature, a.avg_humidity, a.avg_rain, a.max_rain, a.min_rain, 
a.avg_wind_speed, a.max_wind_speed, a.min_wind_speed, 
b.location, b.incident, b.avg_delay, b.min_delay, b.max_delay, 
b.count_delay, b.avg_gap, b.min_gap, b.max_gap
INTO production_weekday_weekend_hour_full
FROM staging_weather_weekday_weekend_hour a
LEFT JOIN staging_bus_delay_weekday_weekend_hour_location_incident b
ON a.day_type = b.day_type
AND a.hour = b.hour;


SELECT DISTINCT day_type
INTO day_types
FROM production_weekday_weekend_hour;

SELECT *
INTO day_type_hour
FROM day_types CROSS JOIN hours;

ALTER TABLE production_weekday_weekend_hour
ADD COLUMN location_id bigint;

ALTER TABLE production_weekday_weekend_hour
ADD CONSTRAINT fk_incident_id
FOREIGN KEY (incident_id) REFERENCES incidents(id);