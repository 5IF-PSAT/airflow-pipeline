# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import os
import psycopg2
import time
import argparse
import redis
import json

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--year', type=str, default='2017', help='Year to process')

args = arg_parser.parse_args()
year = args.year

POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_USERNAME = os.environ.get('POSTGRES_USERNAME', 'postgres')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')
POSTGRES_DB_STAGING = os.environ.get('POSTGRES_DB_STAGING', 'deng_staging')
POSTGRES_DB_PRODUCTION = os.environ.get('POSTGRES_DB_PRODUCTION', 'deng_production')

REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')


def create_star_schema():
    conn_stag = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB_STAGING
    )
    cursor_stag = conn_stag.cursor()
    search_query = f"""
        SELECT * FROM joined_bus_weather_{year}
        ORDER BY month, day_type, hour ASC;
    """
    cursor_stag.execute(search_query)
    results = cursor_stag.fetchall()
    conn_prod = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB_PRODUCTION
    )
    cursor_prod = conn_prod.cursor()
    for result in results:
        search_time_query = """
            SELECT id FROM production_time_dimension WHERE year = %s AND month = %s AND day_type = %s AND hour = %s;
        """
        cursor_prod.execute(search_time_query, (int(year), result[1], result[2], result[3]))
        time_id = cursor_prod.fetchone()[0]
        search_location_query = """
            SELECT id FROM production_location_dimension WHERE name = %s;
        """
        cursor_prod.execute(search_location_query, (result[4],))
        location_id = cursor_prod.fetchone()[0]
        search_incident_query = """
            SELECT id FROM production_incident_dimension WHERE name = %s;
        """
        cursor_prod.execute(search_incident_query, (result[5],))
        incident_id = cursor_prod.fetchone()[0]
        insert_query = """
            INSERT INTO production_bus_weather_fact (time_id, location_id, incident_id, avg_temperature, 
            min_temperature, max_temperature, avg_humidity, avg_rain, max_rain, min_rain, avg_wind_speed, 
            max_wind_speed, min_wind_speed, avg_delay, min_delay, max_delay, count_delay, avg_gap, min_gap, 
            max_gap, count_gap) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s)
        """
        cursor_prod.execute(insert_query, (time_id, location_id, incident_id, result[6], result[7], result[8], result[9],
                                      result[10], result[11], result[12], result[13], result[14], result[15],
                                      result[16], result[17], result[18], result[19], result[20], result[21],
                                      result[22], result[23]))
        conn_prod.commit()
    cursor_stag.close()
    conn_stag.close()
    cursor_prod.close()
    conn_prod.close()


# def extract_data_to_csv():
#     conn_prod = psycopg2.connect(
#         host=POSTGRES_HOST,
#         port=POSTGRES_PORT,
#         user=POSTGRES_USERNAME,
#         password=POSTGRES_PASSWORD,
#         database=POSTGRES_DB_PRODUCTION
#     )
#     cursor_prod = conn_prod.cursor()
#     extract_query = """
#         COPY production_time_dimension TO '/var/lib/postgresql/data/time_dim.csv' WITH CSV HEADER;
#         COPY production_location_dimension TO '/var/lib/postgresql/data/location_dim.csv' WITH CSV HEADER;
#         COPY production_incident_dimension TO '/var/lib/postgresql/data/incident_dim.csv' WITH CSV HEADER;
#         COPY production_bus_weather_fact TO '/var/lib/postgresql/data/fact_table.csv' WITH CSV HEADER;
#     """
#     cursor_prod.execute(extract_query)
#     cursor_prod.close()
#     conn_prod.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Check if data is cached in Redis
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1)
    if redis_client.exists(f'data_star_schema_{year}'):
        print('Data is cached in Redis')
        data = redis_client.get(f'data_star_schema_{year}')
        data = json.loads(data)
        print(data)
    else:
        print('Data is not cached in Redis')
        start_time = time.time()
        create_star_schema()
        # Cache data in Redis
        redis_client.set(f'data_star_schema_{year}', json.dumps({'status': 'done', 'duration': f'{time.time() - start_time}'}))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
