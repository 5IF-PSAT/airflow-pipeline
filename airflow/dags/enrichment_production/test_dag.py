import airflow
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

default_args = {
    'owner': 'minh.ngo',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'minh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'test_docker_dag',
    default_args=default_args,
    description='Test Docker DAG',
    schedule_interval=None,
)

insert_data = DockerOperator(
    task_id='insert_data',
    mount_tmp_dir=False,
    image='nmngo248/star-schema:latest',
    network_mode='meteorif',
    auto_remove=True,
    xcom_all=True,
    api_version='auto',
    dag=dag,
    trigger_rule='none_failed'
)

insert_data