# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import os
import psycopg2
from geopy.geocoders import Nominatim

POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_USERNAME = os.environ.get('POSTGRES_USERNAME', 'postgres')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'deng')


def insert_data_time_dimension():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    cursor = conn.cursor()
    search_query = "SELECT DISTINCT year, month, day_type, hour FROM joined_bus_weather"
    cursor.execute(search_query)
    results = cursor.fetchall()
    for result in results:
        insert_query = """INSERT INTO production_time_dimension (year, month, day_type, hour) 
            VALUES (%s, %s, %s, %s)
            """
        cursor.execute(insert_query, result)
        conn.commit()
    cursor.close()
    conn.close()


def insert_data_location_dimension():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    cursor = conn.cursor()
    search_query = "SELECT DISTINCT location FROM joined_bus_weather"
    cursor.execute(search_query)
    results = cursor.fetchall()
    geolocator = Nominatim(user_agent="your_app_name")
    for result in results:
        location_string = result[0] + ", Toronto, Ontario, Canada"
        location_info = geolocator.geocode(location_string)
        if location_info:
            insert_query = """INSERT INTO production_location_dimension (name, latitude, longitude) 
                VALUES (%s, %s, %s)
                """
            result = (result[0], location_info.latitude, location_info.longitude)
        else:
            insert_query = """INSERT INTO production_location_dimension (name) 
                VALUES (%s)
                """
            result = (result[0],)
        cursor.execute(insert_query, result)
        conn.commit()
    cursor.close()
    conn.close()


def insert_data_incident_dimension():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    cursor = conn.cursor()
    search_query = "SELECT DISTINCT incident FROM joined_bus_weather"
    cursor.execute(search_query)
    results = cursor.fetchall()
    for result in results:
        insert_query = """INSERT INTO production_incident_dimension (name) 
            VALUES (%s)
            """
        cursor.execute(insert_query, result)
        conn.commit()
    cursor.close()
    conn.close()


def create_star_schema():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    cursor = conn.cursor()
    search_query = """
        SELECT * FROM joined_bus_weather;
    """
    cursor.execute(search_query)
    results = cursor.fetchall()
    for result in results:
        search_time_query = """
            SELECT id FROM production_time_dimension WHERE year = %s AND month = %s AND day_type = %s AND hour = %s;
        """
        cursor.execute(search_time_query, (result[0], result[1], result[2], result[3]))
        time_id = cursor.fetchone()[0]
        search_location_query = """
            SELECT id FROM production_location_dimension WHERE name = %s;
        """
        cursor.execute(search_location_query, (result[4],))
        location_id = cursor.fetchone()[0]
        search_incident_query = """
            SELECT id FROM production_incident_dimension WHERE name = %s;
        """
        cursor.execute(search_incident_query, (result[5],))
        incident_id = cursor.fetchone()[0]
        insert_query = """
            INSERT INTO production_bus_weather_fact (time_id, location_id, incident_id, avg_temperature, 
            min_temperature, max_temperature, avg_humidity, avg_rain, max_rain, min_rain, avg_wind_speed, 
            max_wind_speed, min_wind_speed, avg_delay, min_delay, max_delay, count_delay, avg_gap, min_gap, 
            max_gap, count_gap) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s)
        """
        cursor.execute(insert_query, (time_id, location_id, incident_id, result[6], result[7], result[8], result[9],
                                      result[10], result[11], result[12], result[13], result[14], result[15],
                                      result[16], result[17], result[18], result[19], result[20], result[21],
                                      result[22], result[23]))
        conn.commit()
    cursor.close()
    conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    insert_data_time_dimension()
    insert_data_location_dimension()
    insert_data_incident_dimension()
    create_star_schema()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
