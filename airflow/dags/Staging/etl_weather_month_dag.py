import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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
    'staging_etl_weather_month_dag',
    default_args=default_args,
    description='Staging ETL weather month DAG',
    schedule_interval=None,
)

month = '1'
task01 = BashOperator(
    task_id='staging_weather_january',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '2'
task02 = BashOperator(
    task_id='staging_weather_february',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '3'
task03 = BashOperator(
    task_id='staging_weather_march',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '4'
task04 = BashOperator(
    task_id='staging_weather_april',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '5'
task05 = BashOperator(
    task_id='staging_weather_may',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '6'
task06 = BashOperator(
    task_id='staging_weather_june',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '7'
task07 = BashOperator(
    task_id='staging_weather_july',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '8'
task08 = BashOperator(
    task_id='staging_weather_august',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '9'
task09 = BashOperator(
    task_id='staging_weather_september',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '10'
task10 = BashOperator(
    task_id='staging_weather_october',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '11'
task11 = BashOperator(
    task_id='staging_weather_november',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

month = '12'
task12 = BashOperator(
    task_id='staging_weather_december',
    bash_command=f'curl \"{HOST}:{PORT}/staging_month_weather/?month={month}\"',
    dag=dag,
)

final_task = BashOperator(
    task_id='final_task',
    bash_command='echo "All done!"',
    dag=dag,
    trigger_rule='all_success',
)

[task01, task02, task03, task04, task05, task06, task07, task08, task09, task10, task11, task12] >> final_task