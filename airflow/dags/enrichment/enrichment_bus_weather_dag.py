import airflow
from airflow import DAG
from docker.types import Mount
from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

HOST = 'pipeline-api'
PORT = '8000'

default_args = {
    'owner': 'minh.ngo',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'minh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'enrichment_dag',
    default_args=default_args,
    description='Enrichment Bus Weather DAG',
    schedule_interval=None,
)

create_table = PostgresOperator(
    task_id='create_join_table',
    postgres_conn_id='postgres_staging',
    sql=f"""
        DROP TABLE IF EXISTS joined_bus_weather CASCADE;
        CREATE TABLE IF NOT EXISTS joined_bus_weather (
            year integer NOT NULL,
            month integer NOT NULL,
            day_type varchar(255) NOT NULL,
            hour integer NOT NULL,
            location varchar(255) NOT NULL,
            incident varchar(255) NOT NULL,
            avg_temperature double precision,
            min_temperature double precision,
            max_temperature double precision,
            avg_humidity double precision,
            avg_rain double precision,
            max_rain double precision,
            min_rain double precision,
            avg_wind_speed double precision,
            max_wind_speed double precision,
            min_wind_speed double precision,
            avg_delay double precision,
            min_delay double precision,
            max_delay double precision,
            count_delay integer,
            avg_gap double precision,
            min_gap double precision,
            max_gap double precision,
            count_gap integer
        ) PARTITION BY LIST (year);

        CREATE TABLE IF NOT EXISTS joined_bus_weather_2017 PARTITION OF
        joined_bus_weather FOR VALUES IN (2017);

        CREATE TABLE IF NOT EXISTS joined_bus_weather_2018 PARTITION OF
        joined_bus_weather FOR VALUES IN (2018);

        CREATE TABLE IF NOT EXISTS joined_bus_weather_2019 PARTITION OF
        joined_bus_weather FOR VALUES IN (2019);

        CREATE TABLE IF NOT EXISTS joined_bus_weather_2020 PARTITION OF
        joined_bus_weather FOR VALUES IN (2020);

        CREATE TABLE IF NOT EXISTS joined_bus_weather_2021 PARTITION OF
        joined_bus_weather FOR VALUES IN (2021);

        CREATE TABLE IF NOT EXISTS joined_bus_weather_2022 PARTITION OF
        joined_bus_weather FOR VALUES IN (2022);

        CREATE INDEX IF NOT EXISTS joined_bus_weather_year_idx
        ON joined_bus_weather (year, month, day_type, hour);
    """,
    dag=dag,
)

enrich_bus_weather = BashOperator(
    task_id='enrich_bus_weather',
    bash_command=f'/opt/scripts/enrich_bus_weather.sh {HOST} {PORT}',
    dag=dag,
    trigger_rule='none_failed',
)

create_table >> enrich_bus_weather