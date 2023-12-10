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
    'get_fact_table_dag',
    default_args=default_args,
    description='Get fact table data',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

create_enrichment_bus = SparkSubmitOperator(
    task_id='create_enrichment_bus',
    application='/opt/bitnami/spark/app/production/create_enrichment_bus.py',
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    jars='/opt/bitnami/spark/jars/postgresql-42.2.23.jar',
    driver_class_path='/opt/bitnami/spark/jars/postgresql-42.2.23.jar',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

join_bus_weather = SparkSubmitOperator(
    task_id='join_bus_weather',
    application='/opt/bitnami/spark/app/production/join_bus_weather.py',
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    jars='/opt/bitnami/spark/jars/postgresql-42.2.23.jar',
    driver_class_path='/opt/bitnami/spark/jars/postgresql-42.2.23.jar',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

join_fact_dimension = SparkSubmitOperator(
    task_id='join_fact_dimension',
    application='/opt/bitnami/spark/app/production/join_fact_dimension.py',
    conn_id='spark_default',
    dag=dag,
    trigger_rule='none_failed',
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

end = DummyOperator(task_id='end', dag=dag)

start >> create_enrichment_bus >> join_bus_weather >> join_fact_dimension >> end