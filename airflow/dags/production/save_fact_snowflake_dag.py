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
    'save_fact_table_snowflake_dag',
    default_args=default_args,
    description='Save fact table to Snowflake',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

create_fact_table = SnowflakeOperator(
    task_id='create_fact_table',
    sql="""
        CREATE OR REPLACE TABLE DENG_PRODUCTION.DATA_PROD.FACT_TABLE (
            DELAY NUMBER(38,0),
            GAP NUMBER(38,0),
            DIRECTION VARCHAR(50),
            VEHICLE NUMBER(38,0),
            TEMPERATURE NUMBER(38,0),
            HUMIDITY NUMBER(38,0),
            PRECIPITATION NUMBER(38,0),
            RAIN NUMBER(38,0),
            SNOWFALL NUMBER(38,0),
            WINDSPEED NUMBER(38,0),
            INCIDENT_ID NUMBER(38,0),
            TIME_ID NUMBER(38,0),
            LOCATION_ID NUMBER(38,0)
        );
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag
)

save_fact_table_snowflake = SparkSubmitOperator(
    task_id='save_fact_table_snowflake',
    application='/opt/bitnami/spark/app/production/save_fact_table_snowflake.py',
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    packages='net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

end = DummyOperator(task_id='end', dag=dag)

start >> create_fact_table >> save_fact_table_snowflake >> end