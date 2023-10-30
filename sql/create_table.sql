CREATE TABLE IF NOT EXISTS staging_weather_year (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    year integer,
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

CREATE TABLE IF NOT EXISTS staging_weather_month (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    month integer,
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

CREATE TABLE IF NOT EXISTS staging_weather_hour (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    hour integer,
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

CREATE TABLE IF NOT EXISTS staging_weather_year_month (
    id varchar(255) NOT NULL PRIMARY KEY,
    year integer,
    month integer,
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

CREATE TABLE IF NOT EXISTS staging_weather_weekday_weekend (
    id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    day_type varchar(255),
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