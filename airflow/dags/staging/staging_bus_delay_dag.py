import airflow
from airflow import DAG
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
    'staging_bus_delay_dag',
    default_args=default_args,
    description='Staging Bus Delay DAG',
    schedule_interval=None,
)

create_table_task = PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id='postgres_default',
    sql=f"""
        CREATE TABLE IF NOT EXISTS staging_bus_delay (
            year integer,
            month integer,
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
        ) PARTITION BY LIST (year);

        CREATE TABLE IF NOT EXISTS staging_bus_delay_2017 PARTITION OF 
        staging_bus_delay FOR VALUES IN (2017);

        CREATE TABLE IF NOT EXISTS staging_bus_delay_2018 PARTITION OF
        staging_bus_delay FOR VALUES IN (2018);

        CREATE TABLE IF NOT EXISTS staging_bus_delay_2019 PARTITION OF
        staging_bus_delay FOR VALUES IN (2019);

        CREATE TABLE IF NOT EXISTS staging_bus_delay_2020 PARTITION OF
        staging_bus_delay FOR VALUES IN (2020);

        CREATE TABLE IF NOT EXISTS staging_bus_delay_2021 PARTITION OF
        staging_bus_delay FOR VALUES IN (2021);

        CREATE TABLE IF NOT EXISTS staging_bus_delay_2022 PARTITION OF
        staging_bus_delay FOR VALUES IN (2022);

        CREATE INDEX IF NOT EXISTS staging_bus_delay_year_idx 
        ON staging_bus_delay (year);
    """,
    dag=dag,
    autocommit=True,
)

charge_data = BashOperator(
    task_id='charge_data_postgres',
    bash_command=f'curl \"{HOST}:{PORT}/staging_full_bus_delay/\"',
    dag=dag,
    trigger_rule='none_failed',
)

create_table_task >> charge_data