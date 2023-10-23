# Bordeaux: (44.8389957, -0.5692577999999999)
# Bourgogne: (47.151159, 4.892087)

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

HOST = 'event-streaming'
PORT = '8000'
LATITUDE = '44.8389957'
LONGITUDE = '-0.5692577999999999'
CITY = 'bordeaux'

default_args = {
    'owner': 'minh.ngo',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'ngocminh@mail.com',
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='etl_weather_dag',
    default_args=default_args,
    schedule_interval=None,
)

# Get weather data from 01/01/2017 to 31/03/2017
topic_name = 'extract-weather-20171'
start_date = '2017-01-01'
end_date = '2017-03-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task11 = BashOperator(
    task_id='get_weather_data_20171',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/04/2017 to 30/06/2017
topic_name = 'extract-weather-20172'
start_date = '2017-04-01'
end_date = '2017-06-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task12 = BashOperator(
    task_id='get_weather_data_20172',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/07/2017 to 30/09/2017
topic_name = 'extract-weather-20173'
start_date = '2017-07-01'
end_date = '2017-09-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task13 = BashOperator(
    task_id='get_weather_data_20173',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/10/2017 to 31/12/2017
topic_name = 'extract-weather-20174'
start_date = '2017-10-01'
end_date = '2017-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task14 = BashOperator(
    task_id='get_weather_data_20174',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/01/2018 to 31/03/2018
topic_name = 'extract-weather-20181'
start_date = '2018-01-01'
end_date = '2018-03-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task21 = BashOperator(
    task_id='get_weather_data_20181',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/04/2018 to 30/06/2018
topic_name = 'extract-weather-20182'
start_date = '2018-04-01'
end_date = '2018-06-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task22 = BashOperator(
    task_id='get_weather_data_20182',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/07/2018 to 30/09/2018
topic_name = 'extract-weather-20183'
start_date = '2018-07-01'
end_date = '2018-09-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task23 = BashOperator(
    task_id='get_weather_data_20183',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/10/2018 to 31/12/2018
topic_name = 'extract-weather-20184'
start_date = '2018-10-01'
end_date = '2018-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task24 = BashOperator(
    task_id='get_weather_data_20184',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/01/2019 to 31/03/2019
topic_name = 'extract-weather-20191'
start_date = '2019-01-01'
end_date = '2019-03-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task31 = BashOperator(
    task_id='get_weather_data_20191',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/04/2019 to 30/06/2019
topic_name = 'extract-weather-20192'
start_date = '2019-04-01'
end_date = '2019-06-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task32 = BashOperator(
    task_id='get_weather_data_20192',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/07/2019 to 30/09/2019
topic_name = 'extract-weather-20193'
start_date = '2019-07-01'
end_date = '2019-09-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task33 = BashOperator(
    task_id='get_weather_data_20193',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/10/2019 to 31/12/2019
topic_name = 'extract-weather-20194'
start_date = '2019-10-01'
end_date = '2019-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task34 = BashOperator(
    task_id='get_weather_data_20194',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/01/2020 to 31/03/2020
topic_name = 'extract-weather-20201'
start_date = '2020-01-01'
end_date = '2020-03-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task41 = BashOperator(
    task_id='get_weather_data_20201',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/04/2020 to 30/06/2020
topic_name = 'extract-weather-20202'
start_date = '2020-04-01'
end_date = '2020-06-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task42 = BashOperator(
    task_id='get_weather_data_20202',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/07/2020 to 30/09/2020
topic_name = 'extract-weather-20203'
start_date = '2020-07-01'
end_date = '2020-09-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task43 = BashOperator(
    task_id='get_weather_data_20203',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/10/2020 to 31/12/2020
topic_name = 'extract-weather-20204'
start_date = '2020-10-01'
end_date = '2020-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task44 = BashOperator(
    task_id='get_weather_data_20204',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/01/2021 to 31/03/2021
topic_name = 'extract-weather-20211'
start_date = '2021-01-01'
end_date = '2021-03-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task51 = BashOperator(
    task_id='get_weather_data_20211',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/04/2021 to 30/06/2021
topic_name = 'extract-weather-20212'
start_date = '2021-04-01'
end_date = '2021-06-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task52 = BashOperator(
    task_id='get_weather_data_20212',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/07/2021 to 30/09/2021
topic_name = 'extract-weather-20213'
start_date = '2021-07-01'
end_date = '2021-09-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task53 = BashOperator(
    task_id='get_weather_data_20213',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/10/2021 to 31/12/2021
topic_name = 'extract-weather-20214'
start_date = '2021-10-01'
end_date = '2021-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task54 = BashOperator(
    task_id='get_weather_data_20214',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/01/2022 to 31/03/2022
topic_name = 'extract-weather-20221'
start_date = '2022-01-01'
end_date = '2022-03-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task61 = BashOperator(
    task_id='get_weather_data_20221',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/04/2022 to 30/06/2022
topic_name = 'extract-weather-20222'
start_date = '2022-04-01'
end_date = '2022-06-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task62 = BashOperator(
    task_id='get_weather_data_20222',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/07/2022 to 30/09/2022
topic_name = 'extract-weather-20223'
start_date = '2022-07-01'
end_date = '2022-09-30'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task63 = BashOperator(
    task_id='get_weather_data_20223',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Get weather data from 01/10/2022 to 31/12/2022
topic_name = 'extract-weather-20224'
start_date = '2022-10-01'
end_date = '2022-12-31'
file_name = start_date.replace('-', '') + '-' + end_date.replace('-', '')
task64 = BashOperator(
    task_id='get_weather_data_20224',
    bash_command=f'curl \"{HOST}:{PORT}/getETLWeatherDataPipeline/?topic_name={topic_name}&lat={LATITUDE}&lon={LONGITUDE}&start_date={start_date}&end_date={end_date}&file_name={file_name}&city={CITY}\" --output /opt/airflow/dags/data/{CITY}/hourly/{file_name}.csv',
    dag=dag,
    trigger_rule='none_failed'
)

# Final task
final_task = BashOperator(
    task_id='final_task',
    bash_command='echo \"All done!\"',
    dag=dag,
    trigger_rule='all_success'
)

task11 >> task12 >> task13 >> task14
task21 >> task22 >> task23 >> task24
task31 >> task32 >> task33 >> task34
task41 >> task42 >> task43 >> task44
task51 >> task52 >> task53 >> task54
task61 >> task62 >> task63 >> task64
[task14, task24, task34, task44, task54, task64] >> final_task
