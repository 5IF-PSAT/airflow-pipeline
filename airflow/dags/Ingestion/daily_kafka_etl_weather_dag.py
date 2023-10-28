import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

HOST = 'event-streaming'
PORT = '8000'
LATITUDE = '43.653226' # Toronto
LONGITUDE = '-79.383184'
CITY = 'Toronto'

default_args = {
    'owner': 'minh.ngo',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'ngocminh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'daily_kafka_etl_weather_dag',
    default_args=default_args,
    description='Daily Kafka ETL weather DAG',
    schedule_interval=None,
)

# Get weather data from 01/01/2017 to 31/12/2017
start_date = '2017-01-01'
end_date = '2017-12-31'
transform_topic_name = 'transform-topic-1'
load_topic_name = 'load-topic-1'
task1 = BashOperator(
    task_id='get_weather_data_2017',
    bash_command=f'curl \"{HOST}:{PORT}/kafka_ingestion_weather/?transform_topic_name={transform_topic_name}&load_topic_name={load_topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&city={CITY}&daily=yes\"',
    dag=dag,
)

# Get weather data from 01/01/2018 to 31/12/2018
start_date = '2018-01-01'
end_date = '2018-12-31'
transform_topic_name = 'transform-topic-2'
load_topic_name = 'load-topic-2'
task2 = BashOperator(
    task_id='get_weather_data_2018',
    bash_command=f'curl \"{HOST}:{PORT}/kafka_ingestion_weather/?transform_topic_name={transform_topic_name}&load_topic_name={load_topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&city={CITY}&daily=yes\"',
    dag=dag,
)

# Get weather data from 01/01/2019 to 31/12/2019
start_date = '2019-01-01'
end_date = '2019-12-31'
transform_topic_name = 'transform-topic-3'
load_topic_name = 'load-topic-3'
task3 = BashOperator(
    task_id='get_weather_data_2019',
    bash_command=f'curl \"{HOST}:{PORT}/kafka_ingestion_weather/?transform_topic_name={transform_topic_name}&load_topic_name={load_topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&city={CITY}&daily=yes\"',
    dag=dag,
)

# Get weather data from 01/01/2020 to 31/12/2020
start_date = '2020-01-01'
end_date = '2020-12-31'
transform_topic_name = 'transform-topic-4'
load_topic_name = 'load-topic-4'
task4 = BashOperator(
    task_id='get_weather_data_2020',
    bash_command=f'curl \"{HOST}:{PORT}/kafka_ingestion_weather/?transform_topic_name={transform_topic_name}&load_topic_name={load_topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&city={CITY}&daily=yes\"',
    dag=dag,
)

# Get weather data from 01/01/2021 to 31/12/2021
start_date = '2021-01-01'
end_date = '2021-12-31'
transform_topic_name = 'transform-topic-5'
load_topic_name = 'load-topic-5'
task5 = BashOperator(
    task_id='get_weather_data_2021',
    bash_command=f'curl \"{HOST}:{PORT}/kafka_ingestion_weather/?transform_topic_name={transform_topic_name}&load_topic_name={load_topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&city={CITY}&daily=yes\"',
    dag=dag,
)

# Get weather data from 01/01/2022 to 31/12/2022
start_date = '2022-01-01'
end_date = '2022-12-31'
transform_topic_name = 'transform-topic-6'
load_topic_name = 'load-topic-6'
task6 = BashOperator(
    task_id='get_weather_data_2022',
    bash_command=f'curl \"{HOST}:{PORT}/kafka_ingestion_weather/?transform_topic_name={transform_topic_name}&load_topic_name={load_topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&city={CITY}&daily=yes\"',
    dag=dag,
)

# Final task
final_task = BashOperator(
    task_id='final_task',
    bash_command='echo "Kafka ETL weather data completed"',
    dag=dag,
    trigger_rule='all_success'
)

# Set dependencies
[task1, task2, task3, task4, task5, task6] >> final_task