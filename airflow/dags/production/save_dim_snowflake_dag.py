from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'minh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0}

dag = DAG(
    'save_dim_snowflake_dag',
    default_args=default_args,
    description='Save dimension to Snowflake',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

create_time_dim_table = SnowflakeOperator(
    task_id='create_time_dim_table',
    sql="""
        CREATE OR REPLACE TABLE DENG_PRODUCTION.DATA_PROD.TIME_DIM (
            ID NUMBER(38,0),
            YEAR NUMBER(38,0),
            MONTH NUMBER(38,0),
            DAY NUMBER(38,0),
            DAY_OF_WEEK VARCHAR(50),
            DAY_TYPE VARCHAR(50),
            HOUR NUMBER(38,0)
        );
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag
)

save_time_dim_snowflake = SparkSubmitOperator(
    task_id='save_time_dim_snowflake',
    application='/opt/bitnami/spark/app/production/save_dim_snowflake.py',
    application_args=['time'],
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    packages='net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

create_location_dim_table = SnowflakeOperator(
    task_id='create_location_dim_table',
    sql="""
        CREATE OR REPLACE TABLE DENG_PRODUCTION.DATA_PROD.LOCATION_DIM (
            ID NUMBER(38,0),
            LOCATION VARCHAR(50),
            LOCATION_SLUG VARCHAR(50),
            LATITUDE NUMBER(38,0),
            LONGITUDE NUMBER(38,0)
        );
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag
)

save_location_dim_snowflake = SparkSubmitOperator(
    task_id='save_location_dim_snowflake',
    application='/opt/bitnami/spark/app/production/save_dim_snowflake.py',
    application_args=['location'],
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    packages='net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

create_incident_dim_table = SnowflakeOperator(
    task_id='create_incident_dim_table',
    sql="""
        CREATE OR REPLACE TABLE DENG_PRODUCTION.DATA_PROD.INCIDENT_DIM (
            ID NUMBER(38,0),
            INCIDENT VARCHAR(50),
            INCIDENT_SLUG VARCHAR(50)
        );
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag
)

save_incident_dim_snowflake = SparkSubmitOperator(
    task_id='save_incident_dim_snowflake',
    application='/opt/bitnami/spark/app/production/save_dim_snowflake.py',
    application_args=['incident'],
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    packages='net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

end = DummyOperator(task_id='end', dag=dag, trigger_rule='all_success')

start >> create_time_dim_table >> save_time_dim_snowflake
start >> create_location_dim_table >> save_location_dim_snowflake
start >> create_incident_dim_table >> save_incident_dim_snowflake
[save_time_dim_snowflake, save_location_dim_snowflake, save_incident_dim_snowflake] >> end