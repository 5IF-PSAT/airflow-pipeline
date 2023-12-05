import airflow
from airflow import DAG
from datetime import timedelta, datetime
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
    'get_location_dag',
    default_args=default_args,
    description='Get location data',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

clean_location_2017 = SparkSubmitOperator(
    task_id='clean_location_2017',
    application='/opt/bitnami/spark/app/enrichment/clean_location.py',
    conn_id='spark_default',
    application_args=['2017'],
    dag=dag
)

get_location_2017 = SparkSubmitOperator(
    task_id='get_location_2017',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    conn_id='spark_default',
    application_args=['2017'],
    dag=dag,
    trigger_rule='none_failed'
)

clean_location_2018 = SparkSubmitOperator(
    task_id='clean_location_2018',
    application='/opt/bitnami/spark/app/enrichment/clean_location.py',
    conn_id='spark_default',
    application_args=['2018'],
    dag=dag
)

get_location_2018 = SparkSubmitOperator(
    task_id='get_location_2018',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    conn_id='spark_default',
    application_args=['2018'],
    dag=dag,
    trigger_rule='none_failed'
)

clean_location_2019 = SparkSubmitOperator(
    task_id='clean_location_2019',
    application='/opt/bitnami/spark/app/enrichment/clean_location.py',
    conn_id='spark_default',
    application_args=['2019'],
    dag=dag
)

get_location_2019 = SparkSubmitOperator(
    task_id='get_location_2019',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    conn_id='spark_default',
    application_args=['2019'],
    dag=dag,
    trigger_rule='none_failed'
)

clean_location_2020 = SparkSubmitOperator(
    task_id='clean_location_2020',
    application='/opt/bitnami/spark/app/enrichment/clean_location.py',
    conn_id='spark_default',
    application_args=['2020'],
    dag=dag
)

get_location_2020 = SparkSubmitOperator(
    task_id='get_location_2020',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    conn_id='spark_default',
    application_args=['2020'],
    dag=dag,
    trigger_rule='none_failed'
)

clean_location_2021 = SparkSubmitOperator(
    task_id='clean_location_2021',
    application='/opt/bitnami/spark/app/enrichment/clean_location.py',
    conn_id='spark_default',
    application_args=['2021'],
    dag=dag
)

get_location_2021 = SparkSubmitOperator(
    task_id='get_location_2021',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    conn_id='spark_default',
    application_args=['2021'],
    dag=dag,
    trigger_rule='none_failed'
)

clean_location_2022 = SparkSubmitOperator(
    task_id='clean_location_2022',
    application='/opt/bitnami/spark/app/enrichment/clean_location.py',
    conn_id='spark_default',
    application_args=['2022'],
    dag=dag
)

get_location_2022 = SparkSubmitOperator(
    task_id='get_location_2022',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    conn_id='spark_default',
    application_args=['2022'],
    dag=dag,
    trigger_rule='none_failed'
)

end = DummyOperator(task_id='end', dag=dag, trigger_rule='all_success')

# DAG dependencies
start >> clean_location_2017 >> get_location_2017
start >> clean_location_2018 >> get_location_2018
start >> clean_location_2019 >> get_location_2019
start >> clean_location_2020 >> get_location_2020
start >> clean_location_2021 >> get_location_2021
start >> clean_location_2022 >> get_location_2022
[get_location_2017, get_location_2018, get_location_2019, get_location_2020, get_location_2021, get_location_2022] >> end