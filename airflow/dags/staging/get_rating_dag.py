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
    'get_rating_dag',
    default_args=default_args,
    description='Get rating data',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    conn_id='spark_default',
    application='/opt/bitnami/spark/app/ingestion/get_wines.py',
    name='spark_submit_task',
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# set dependencies
start >> spark_submit_task >> end