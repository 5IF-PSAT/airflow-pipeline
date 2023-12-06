from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'minh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'get_time_dag',
    default_args=default_args,
    description='Get time data',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

get_time_task = SparkSubmitOperator(
    task_id='get_time_task',
    application='/opt/bitnami/spark/app/enrichment/get_time.py',
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

end = DummyOperator(task_id='end', dag=dag)

start >> get_time_task >> end