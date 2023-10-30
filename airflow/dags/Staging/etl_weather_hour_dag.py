import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

HOST = 'event-streaming'
PORT = '8000'

default_args = {
    'owner': 'minh.ngo',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'minh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'staging_etl_weather_hour_dag',
    default_args=default_args,
    description='Staging ETL weather hour DAG',
    schedule_interval=None,
)

hour = '0'
task01 = BashOperator(
    task_id='staging_weather_0',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
)

hour = '1'
task02 = BashOperator(
    task_id='staging_weather_1',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '2'
task03 = BashOperator(
    task_id='staging_weather_2',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '3'
task04 = BashOperator(
    task_id='staging_weather_3',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '4'
task05 = BashOperator(
    task_id='staging_weather_4',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '5'
task06 = BashOperator(
    task_id='staging_weather_5',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '6'
task07 = BashOperator(
    task_id='staging_weather_6',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '7'
task08 = BashOperator(
    task_id='staging_weather_7',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '8'
task09 = BashOperator(
    task_id='staging_weather_8',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '9'
task10 = BashOperator(
    task_id='staging_weather_9',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '10'
task11 = BashOperator(
    task_id='staging_weather_10',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '11'
task12 = BashOperator(
    task_id='staging_weather_11',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '12'
task13 = BashOperator(
    task_id='staging_weather_12',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '13'
task14 = BashOperator(
    task_id='staging_weather_13',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '14'
task15 = BashOperator(
    task_id='staging_weather_14',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '15'
task16 = BashOperator(
    task_id='staging_weather_15',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '16'
task17 = BashOperator(
    task_id='staging_weather_16',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '17'
task18 = BashOperator(
    task_id='staging_weather_17',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '18'
task19 = BashOperator(
    task_id='staging_weather_18',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '19'
task20 = BashOperator(
    task_id='staging_weather_19',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '20'
task21 = BashOperator(
    task_id='staging_weather_20',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '21'
task22 = BashOperator(
    task_id='staging_weather_21',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '22'
task23 = BashOperator(
    task_id='staging_weather_22',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

hour = '23'
task24 = BashOperator(
    task_id='staging_weather_23',
    bash_command=f'curl \"{HOST}:{PORT}/staging_hour_weather/?hour={hour}\"',
    dag=dag,
    trigger_rule='none_failed',
)

# final task
final_task = BashOperator(
    task_id='staging_weather_final',
    bash_command='echo \"All done!\"',
    dag=dag,
    trigger_rule='all_success',
)

# set task dependencies
task01 >> task02 >> task03 >> task04
task05 >> task06 >> task07 >> task08 
task09 >> task10 >> task11 >> task12 
task13 >> task14 >> task15 >> task16
task17 >> task18 >> task19 >> task20 
task21 >> task22 >> task23 >> task24
[task04, task08, task12, task16, task20, task24] >> final_task