from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'minh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'get_incident_dag',
    default_args=default_args,
    description='Get incident data',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

create_table_task = PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id='postgres_staging',
    sql=f"""
        DROP TABLE IF EXISTS enrichment_bus_delay;
        CREATE TABLE IF NOT EXISTS enrichment_bus_delay (
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
            vehicle varchar(255),
            incident_slug varchar(255)
        ) PARTITION BY LIST (year);

        CREATE TABLE IF NOT EXISTS enrichment_bus_delay_2017 PARTITION OF 
        enrichment_bus_delay FOR VALUES IN (2017);

        CREATE TABLE IF NOT EXISTS enrichment_bus_delay_2018 PARTITION OF
        enrichment_bus_delay FOR VALUES IN (2018);

        CREATE TABLE IF NOT EXISTS enrichment_bus_delay_2019 PARTITION OF
        enrichment_bus_delay FOR VALUES IN (2019);

        CREATE TABLE IF NOT EXISTS enrichment_bus_delay_2020 PARTITION OF
        enrichment_bus_delay FOR VALUES IN (2020);

        CREATE TABLE IF NOT EXISTS enrichment_bus_delay_2021 PARTITION OF
        enrichment_bus_delay FOR VALUES IN (2021);

        CREATE TABLE IF NOT EXISTS enrichment_bus_delay_2022 PARTITION OF
        enrichment_bus_delay FOR VALUES IN (2022);

        CREATE INDEX IF NOT EXISTS enrichment_bus_delay_year_idx 
        ON enrichment_bus_delay (year);

        CREATE INDEX IF NOT EXISTS enrichment_bus_delay_full_idx
        ON enrichment_bus_delay (year, month, day_type, hour);

        CREATE INDEX IF NOT EXISTS enrichment_bus_delay_full_2_idx
        ON enrichment_bus_delay (year, month, day, hour);
    """,
    dag=dag,
    autocommit=True,
)

clean_incident = SparkSubmitOperator(
    task_id='clean_incident',
    application='/opt/bitnami/spark/app/enrichment/clean_incident.py',
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    jars='/opt/bitnami/spark/jars/postgresql-42.2.23.jar',
    driver_class_path='/opt/bitnami/spark/jars/postgresql-42.2.23.jar',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_incident = SparkSubmitOperator(
    task_id='get_incident',
    application='/opt/bitnami/spark/app/enrichment/get_incident.py',
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    jars='/opt/bitnami/spark/jars/postgresql-42.2.23.jar',
    driver_class_path='/opt/bitnami/spark/jars/postgresql-42.2.23.jar',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

end = DummyOperator(task_id='end', dag=dag, trigger_rule='all_success')

# DAG dependencies
start >> create_table_task >> clean_incident >> get_incident >> end