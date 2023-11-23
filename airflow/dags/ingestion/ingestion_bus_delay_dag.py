import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

HOST = 'pipeline-api'
PORT = '8000'
CITY = 'Toronto'

default_args = {
    'owner': 'minh.ngo',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'ngocminh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'ingestion_bus_delay_dag',
    default_args=default_args,
    description='Ingestion Bus delay DAG',
    schedule_interval=None,
)

# Get bus delay data from 01/01/2017 to 31/12/2017
year = 2017
task1 = BashOperator(
    task_id='get_bus_delay_data_2017',
    bash_command=f'/opt/scripts/bus_delay.sh {HOST} {PORT} {year} {CITY}',
    dag=dag,
)

# Get bus delay data from 01/01/2018 to 31/12/2018
year = 2018
task2 = BashOperator(
    task_id='get_bus_delay_data_2018',
    bash_command=f'/opt/scripts/bus_delay.sh {HOST} {PORT} {year} {CITY}',
    dag=dag,
)

# Get bus delay data from 01/01/2019 to 31/12/2019
year = 2019
task3 = BashOperator(
    task_id='get_bus_delay_data_2019',
    bash_command=f'/opt/scripts/bus_delay.sh {HOST} {PORT} {year} {CITY}',
    dag=dag,
)

# Get bus delay data from 01/01/2020 to 31/12/2020
year = 2020
task4 = BashOperator(
    task_id='get_bus_delay_data_2020',
    bash_command=f'/opt/scripts/bus_delay.sh {HOST} {PORT} {year} {CITY}',
    dag=dag,
)

# Get bus delay data from 01/01/2021 to 31/12/2021
year = 2021
task5 = BashOperator(
    task_id='get_bus_delay_data_2021',
    bash_command=f'/opt/scripts/bus_delay.sh {HOST} {PORT} {year} {CITY}',
    dag=dag,
)

# Get bus delay data from 01/01/2022 to 31/12/2022
year = 2022
task6 = BashOperator(
    task_id='get_bus_delay_data_2022',
    bash_command=f'/opt/scripts/bus_delay.sh {HOST} {PORT} {year} {CITY}',
    dag=dag,
)

# Final task
final_task = BashOperator(
    task_id='final_task',
    bash_command='echo "ETL bus delay DAG completed"',
    dag=dag,
    trigger_rule='all_success'
)

[task1, task2, task3, task4, task5, task6] >> final_task