import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests

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

def get_bus_delay(year, **kwargs):
    ti = kwargs['ti']
    # Check if data is already cached
    cached_key = f'ingestion_bus_delay_{year}'
    cached_data = ti.xcom_pull(key=cached_key, task_ids='get_bus_delay')
    if cached_data:
        return cached_data
    
    # Fetch data from API
    url = f'http://{HOST}:{PORT}/ingestion_bus_delay/?year={year}&city={CITY}'
    response = requests.get(url)
    data = response.json()
    # Cache data
    ti.xcom_push(key=cached_key, value=data)
    return data

get_bus_delay_2017 = PythonOperator(
    task_id='get_bus_delay_2017',
    python_callable=get_bus_delay,
    op_kwargs={'year': 2017},
    dag=dag,
)

get_bus_delay_2018 = PythonOperator(
    task_id='get_bus_delay_2018',
    python_callable=get_bus_delay,
    op_kwargs={'year': 2018},
    dag=dag,
)

get_bus_delay_2019 = PythonOperator(
    task_id='get_bus_delay_2019',
    python_callable=get_bus_delay,
    op_kwargs={'year': 2019},
    dag=dag,
)

get_bus_delay_2020 = PythonOperator(
    task_id='get_bus_delay_2020',
    python_callable=get_bus_delay,
    op_kwargs={'year': 2020},
    dag=dag,
)

get_bus_delay_2021 = PythonOperator(
    task_id='get_bus_delay_2021',
    python_callable=get_bus_delay,
    op_kwargs={'year': 2021},
    dag=dag,
)

get_bus_delay_2022 = PythonOperator(
    task_id='get_bus_delay_2022',
    python_callable=get_bus_delay,
    op_kwargs={'year': 2022},
    dag=dag,
)

# final task
final_task = BashOperator(
    task_id='final_task',
    bash_command='echo "All done!"',
    dag=dag,
    trigger_rule='all_success'
)

# set dependencies
[get_bus_delay_2017, get_bus_delay_2018, get_bus_delay_2019, get_bus_delay_2020, get_bus_delay_2021, get_bus_delay_2022] >> final_task