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
    'get_location_dag',
    default_args=default_args,
    description='Get location data',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

get_location_1 = SparkSubmitOperator(
    task_id='get_location_1',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["48220", "49220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_2 = SparkSubmitOperator(
    task_id='get_location_2',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["49220", "50220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_3 = SparkSubmitOperator(
    task_id='get_location_3',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["50220", "51220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_4 = SparkSubmitOperator(
    task_id='get_location_4',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["51220", "52220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_5 = SparkSubmitOperator(
    task_id='get_location_5',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["52220", "53220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_6 = SparkSubmitOperator(
    task_id='get_location_6',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["53220", "54220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_7 = SparkSubmitOperator(
    task_id='get_location_7',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["54220", "55220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_8 = SparkSubmitOperator(
    task_id='get_location_8',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["55220", "56220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_9 = SparkSubmitOperator(
    task_id='get_location_9',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["56220", "57220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_10 = SparkSubmitOperator(
    task_id='get_location_10',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["57220", "58220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_11 = SparkSubmitOperator(
    task_id='get_location_11',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["58220", "59220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

get_location_12 = SparkSubmitOperator(
    task_id='get_location_12',
    application='/opt/bitnami/spark/app/enrichment/get_location.py',
    application_args=["59220", "60220"],
    conn_id='spark_default',
    dag=dag,
    conf={'spark.master': 'spark://spark-master:7077'},
    verbose=True
)

end = DummyOperator(task_id='end', dag=dag, trigger_rule='all_success')

# DAG dependencies
start >> get_location_1 >> get_location_4 >> get_location_7 >> get_location_10
start >> get_location_2 >> get_location_5 >> get_location_8 >> get_location_11
start >> get_location_3 >> get_location_6 >> get_location_9 >> get_location_12
[get_location_10, get_location_11, get_location_12] >> end