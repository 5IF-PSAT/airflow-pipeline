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
    'star_schema_dag',
    default_args=default_args,
    description='Star Schema DAG',
    schedule_interval=None,
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
            name varchar(255) NOT NULL,
            latitude double precision,
            longitude double precision
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

insert_time_data = BashOperator(
    task_id='insert_time_dim_data',
    bash_command=f"/opt/scripts/production_time_dim.sh {HOST} {PORT}",
    dag=dag,
    trigger_rule='none_failed',
)

insert_location_data = BashOperator(
    task_id='insert_location_dim_data',
    bash_command=f"/opt/scripts/production_location_dim.sh {HOST} {PORT}",
    dag=dag,
    trigger_rule='none_failed',
)

insert_incident_data = BashOperator(
    task_id='insert_incident_dim_data',
    bash_command=f"/opt/scripts/production_incident_dim.sh {HOST} {PORT}",
    dag=dag,
    trigger_rule='none_failed',
)

year = '2017'
insert_data_2017 = DockerOperator(
    task_id='insert_data_star_schema_2017',
    mount_tmp_dir=False,
    image='nmngo248/star-schema:latest',
    network_mode='meteorif',
    auto_remove=True,
    xcom_all=True,
    command=f'python3 main.py --year {year}',
    api_version='auto',
    docker_url="tcp://docker-socket-proxy:2375",
    dag=dag,
    trigger_rule='none_failed'
)

year = '2018'
insert_data_2018 = DockerOperator(
    task_id='insert_data_star_schema_2018',
    mount_tmp_dir=False,
    image='nmngo248/star-schema:latest',
    network_mode='meteorif',
    auto_remove=True,
    xcom_all=True,
    command=f'python3 main.py --year {year}',
    api_version='auto',
    docker_url="tcp://docker-socket-proxy:2375",
    dag=dag,
    trigger_rule='none_failed'
)

year = '2019'
insert_data_2019 = DockerOperator(
    task_id='insert_data_star_schema_2019',
    mount_tmp_dir=False,
    image='nmngo248/star-schema:latest',
    network_mode='meteorif',
    auto_remove=True,
    xcom_all=True,
    command=f'python3 main.py --year {year}',
    api_version='auto',
    docker_url="tcp://docker-socket-proxy:2375",
    dag=dag,
    trigger_rule='none_failed'
)

year = '2020'
insert_data_2020 = DockerOperator(
    task_id='insert_data_star_schema_2020',
    mount_tmp_dir=False,
    image='nmngo248/star-schema:latest',
    network_mode='meteorif',
    auto_remove=True,
    xcom_all=True,
    command=f'python3 main.py --year {year}',
    api_version='auto',
    docker_url="tcp://docker-socket-proxy:2375",
    dag=dag,
    trigger_rule='none_failed'
)

year = '2021'
insert_data_2021 = DockerOperator(
    task_id='insert_data_star_schema_2021',
    mount_tmp_dir=False,
    image='nmngo248/star-schema:latest',
    network_mode='meteorif',
    auto_remove=True,
    xcom_all=True,
    command=f'python3 main.py --year {year}',
    api_version='auto',
    docker_url="tcp://docker-socket-proxy:2375",
    dag=dag,
    trigger_rule='none_failed'
)

year = '2022'
insert_data_2022 = DockerOperator(
    task_id='insert_data_star_schema_2022',
    mount_tmp_dir=False,
    image='nmngo248/star-schema:latest',
    network_mode='meteorif',
    auto_remove=True,
    xcom_all=True,
    command=f'python3 main.py --year {year}',
    api_version='auto',
    docker_url="tcp://docker-socket-proxy:2375",
    dag=dag,
    trigger_rule='none_failed'
)

create_table_task >> [insert_time_data, insert_location_data, insert_incident_data]
[insert_time_data, insert_location_data, insert_incident_data] >> insert_data_2017
[insert_time_data, insert_location_data, insert_incident_data] >> insert_data_2018
[insert_time_data, insert_location_data, insert_incident_data] >> insert_data_2019
[insert_time_data, insert_location_data, insert_incident_data] >> insert_data_2020
[insert_time_data, insert_location_data, insert_incident_data] >> insert_data_2021
[insert_time_data, insert_location_data, insert_incident_data] >> insert_data_2022