import airflow
from airflow import DAG
from docker.types import Mount
from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.docker.operators.docker import DockerOperator

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
    'enrichment_star_schema_dag',
    default_args=default_args,
    description='Enrichment to create Star Schema DAG',
    schedule_interval=None,
)

enrich_bus_weather = BashOperator(
    task_id='enrich_bus_weather',
    bash_command=f'curl \"{HOST}:{PORT}/join_bus_weather/\"',
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_prod_table_task',
    postgres_conn_id='postgres_production',
    sql=f"""
        DROP TABLE IF EXISTS production_bus_weather_fact CASCADE;
        DROP TABLE IF EXISTS production_time_dimension CASCADE;
        DROP TABLE IF EXISTS production_location_dimension CASCADE;
        DROP TABLE IF EXISTS production_incident_dimension CASCADE;

        CREATE TABLE IF NOT EXISTS production_time_dimension (
            id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            year integer NOT NULL,
            month integer NOT NULL,
            day_type varchar(255) NOT NULL,
            hour integer NOT NULL
        );

        CREATE TABLE IF NOT EXISTS production_location_dimension (
            id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name varchar(255) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS production_incident_dimension (
            id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name varchar(255) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS production_bus_weather_fact (
            id bigint NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            time_id bigint NOT NULL REFERENCES production_time_dimension(id) ON DELETE SET NULL,
            location_id bigint NOT NULL REFERENCES production_location_dimension(id) ON DELETE SET NULL,
            incident_id bigint NOT NULL REFERENCES production_incident_dimension(id) ON DELETE SET NULL,
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
        );

        CREATE INDEX IF NOT EXISTS production_bus_weather_fact_full_idx
        ON production_bus_weather_fact (time_id, location_id, incident_id);
    """,
    dag=dag,
    trigger_rule='none_failed',
)

insert_data = DockerOperator(
    task_id='insert_data_star_schema',
    mount_tmp_dir=False,
    image='nmngo248/star-schema:latest',
    network_mode='meteorif',
    auto_remove=True,
    xcom_all=True,
    api_version='auto',
    docker_url="tcp://docker-socket-proxy:2375",
    dag=dag,
    trigger_rule='none_failed'
)

enrich_bus_weather >> create_table_task >> insert_data