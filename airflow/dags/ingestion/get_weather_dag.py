from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'minh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'get_weather_dag',
    default_args=default_args,
    description='Get Weather DAG',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

spark_submit_task = SparkSubmitOperator(
    task_id='get_weather',
    application='/opt/bitnami/spark/app/ingestion/get_weather.py',
    application_args=['1003'], # Done: 2534
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

end = DummyOperator(task_id='end', dag=dag)

# set dependencies
start >> spark_submit_task >> end