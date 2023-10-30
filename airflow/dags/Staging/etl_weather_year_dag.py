import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import requests

HOST = 'event-streaming'
PORT = '8000'

default_args = {
    'owner': 'minh.ngo',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'minh@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'staging_etl_weather_year_dag',
    default_args=default_args,
    description='Staging ETL weather year DAG',
    schedule_interval=None,
)

start_year = 2017
end_year = 2023
def send_request_to_event_streaming():
    for year in range(start_year, end_year):
        url = f'{HOST}:{PORT}/staging_year_weather/?year={year}'
        response = requests.get(url)
        print(response.text)
        if response.status_code == 200:
            print(f'Successfully send request to {url}')
        else:
            print(f'Failed to send request to {url}')
            return False
    return True

year = 2017
task1 = BashOperator(
    task_id='staging_weather_2017',
    bash_command=f'curl \"{HOST}:{PORT}/staging_year_weather/?year={year}\"',
    dag=dag,
)

year = 2018
task2 = BashOperator(
    task_id='staging_weather_2018',
    bash_command=f'curl \"{HOST}:{PORT}/staging_year_weather/?year={year}\"',
    dag=dag,
)

year = 2019
task3 = BashOperator(
    task_id='staging_weather_2019',
    bash_command=f'curl \"{HOST}:{PORT}/staging_year_weather/?year={year}\"',
    dag=dag,
)

year = 2020
task4 = BashOperator(
    task_id='staging_weather_2020',
    bash_command=f'curl \"{HOST}:{PORT}/staging_year_weather/?year={year}\"',
    dag=dag,
)

year = 2021
task5 = BashOperator(
    task_id='staging_weather_2021',
    bash_command=f'curl \"{HOST}:{PORT}/staging_year_weather/?year={year}\"',
    dag=dag,
)

year = 2022
task6 = BashOperator(
    task_id='staging_weather_2022',
    bash_command=f'curl \"{HOST}:{PORT}/staging_year_weather/?year={year}\"',
    dag=dag,
)

# Finish
final_task = BashOperator(
    task_id='finish',
    bash_command='echo "Finish"',
    dag=dag,
    trigger_rule='all_success',
)

[task1, task2, task3, task4, task5, task6] >> final_task