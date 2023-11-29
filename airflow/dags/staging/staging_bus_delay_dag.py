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
    postgres_conn_id='postgres_staging',
    sql=f"""
        CREATE TABLE IF NOT EXISTS staging_bus_delay (
            year integer,
            month integer,
            day integer NOT NULL,
            day_of_week varchar(255),
            day_type varchar(255) NOT NULL,
            hour integer,
            location varchar(255),
            incident varchar(255),
            delay double precision,
            gap double precision,
            direction varchar(255),
            vehicle varchar(255)
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

        CREATE INDEX IF NOT EXISTS staging_bus_delay_full_idx
        ON staging_bus_delay (year, month, day_type, hour);

        CREATE INDEX IF NOT EXISTS staging_bus_delay_full_2_idx
        ON staging_bus_delay (year, month, day, hour);
    """,
    dag=dag,
    autocommit=True,
)

year = '2017'
charge_data_2017 = BashOperator(
    task_id='charge_data_postgres_2017',
    bash_command=f'/opt/scripts/staging_bus_delay.sh {HOST} {PORT} {year}',
    dag=dag,
    trigger_rule='none_failed',
)

year = '2018'
charge_data_2018 = BashOperator(
    task_id='charge_data_postgres_2018',
    bash_command=f'/opt/scripts/staging_bus_delay.sh {HOST} {PORT} {year}',
    dag=dag,
    trigger_rule='none_failed',
)

year = '2019'
charge_data_2019 = BashOperator(
    task_id='charge_data_postgres_2019',
    bash_command=f'/opt/scripts/staging_bus_delay.sh {HOST} {PORT} {year}',
    dag=dag,
    trigger_rule='none_failed',
)

year = '2020'
charge_data_2020 = BashOperator(
    task_id='charge_data_postgres_2020',
    bash_command=f'/opt/scripts/staging_bus_delay.sh {HOST} {PORT} {year}',
    dag=dag,
    trigger_rule='none_failed',
)

year = '2021'
charge_data_2021 = BashOperator(
    task_id='charge_data_postgres_2021',
    bash_command=f'/opt/scripts/staging_bus_delay.sh {HOST} {PORT} {year}',
    dag=dag,
    trigger_rule='none_failed',
)

year = '2022'
charge_data_2022 = BashOperator(
    task_id='charge_data_postgres_2022',
    bash_command=f'/opt/scripts/staging_bus_delay.sh {HOST} {PORT} {year}',
    dag=dag,
    trigger_rule='none_failed',
)

create_table_task >> [charge_data_2017, charge_data_2018, charge_data_2019, charge_data_2020, charge_data_2021, charge_data_2022]