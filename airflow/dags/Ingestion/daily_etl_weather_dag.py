import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

HOST = 'pipeline-api'
PORT = '8000'
LATITUDE = '43.653226'
LONGITUDE = '-79.383184'
CITY = 'Toronto'
TOPIC_NAME = 'weather-data-pipeline'

default_args = {
    'owner': 'minh.ngo',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'ngocminh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'daily_etl_weather_dag',
    default_args=default_args,
    description='Daily ETL weather DAG',
    schedule_interval=None,
)

# Get weather data from 01/01/2017 to 31/12/2017
start_date = '2017-01-01'
end_date = '2017-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task1 = BashOperator(
    task_id='get_weather_data_2017',
    bash_command=f'/opt/scripts/daily_weather.sh {HOST} {PORT} {LATITUDE} {LONGITUDE} {start_date} {end_date} {file_name} {CITY}',
    dag=dag,
)

# Get weather data from 01/01/2018 to 31/12/2018
start_date = '2018-01-01'
end_date = '2018-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task2 = BashOperator(
    task_id='get_weather_data_2018',
    bash_command=f'/opt/scripts/daily_weather.sh {HOST} {PORT} {LATITUDE} {LONGITUDE} {start_date} {end_date} {file_name} {CITY}',
    dag=dag,
)

# Get weather data from 01/01/2019 to 31/12/2019
start_date = '2019-01-01'
end_date = '2019-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task3 = BashOperator(
    task_id='get_weather_data_2019',
    bash_command=f'/opt/scripts/daily_weather.sh {HOST} {PORT} {LATITUDE} {LONGITUDE} {start_date} {end_date} {file_name} {CITY}',
    dag=dag,
)

# Get weather data from 01/01/2020 to 31/12/2020
start_date = '2020-01-01'
end_date = '2020-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task4 = BashOperator(
    task_id='get_weather_data_2020',
    bash_command=f'/opt/scripts/daily_weather.sh {HOST} {PORT} {LATITUDE} {LONGITUDE} {start_date} {end_date} {file_name} {CITY}',
    dag=dag,
)

# Get weather data from 01/01/2021 to 31/12/2021
start_date = '2021-01-01'
end_date = '2021-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task5 = BashOperator(
    task_id='get_weather_data_2021',
    bash_command=f'/opt/scripts/daily_weather.sh {HOST} {PORT} {LATITUDE} {LONGITUDE} {start_date} {end_date} {file_name} {CITY}',
    dag=dag,
)

# Get weather data from 01/01/2022 to 31/12/2022
start_date = '2022-01-01'
end_date = '2022-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task6 = BashOperator(
    task_id='get_weather_data_2022',
    bash_command=f'/opt/scripts/daily_weather.sh {HOST} {PORT} {LATITUDE} {LONGITUDE} {start_date} {end_date} {file_name} {CITY}',
    dag=dag,
)

# Final task
final_task = BashOperator(
    task_id='final_task',
    bash_command='echo "ETL weather data completed"',
    dag=dag,
    trigger_rule='all_success'
)

# Set dependencies
[task1, task2, task3, task4, task5, task6] >> final_task
