from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
import json
import requests
import os

def extract_weather_data(api_key, output_path):
    url = f"http://api.openweathermap.org/data/2.5/weather?q=Coimbatore&APPID={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, 'w') as outfile:
            json.dump(response.json(), outfile)
    else:
        raise Exception("Failed to fetch weather data")

def transform_weather_data(input_path, output_path):
    def kelvin_to_fahrenheit(temp_in_kelvin):
        return (temp_in_kelvin - 273.15) * (9 / 5) + 32

    with open(input_path, 'r') as infile:
        data = json.load(infile)

    city = data["name"]
    weather_description = data["weather"][0]["description"]
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data["dt"] + data["timezone"])
    sunrise_time = datetime.utcfromtimestamp(data["sys"]["sunrise"] + data["timezone"])
    sunset_time = datetime.utcfromtimestamp(data["sys"]["sunset"] + data["timezone"])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature_F": temp_fahrenheit,
        "Feels_Like_F": feels_like_fahrenheit,
        "Minimum_Temp_F": min_temp_fahrenheit,
        "Maximum_Temp_F": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind_Speed": wind_speed,
        "Time_of_Record": time_of_record,
        "Sunrise_Local_Time": sunrise_time,
        "Sunset_Local_Time": sunset_time
    }
    df = pd.DataFrame([transformed_data])
    df.to_csv(output_path, index=False)

def load_weather_data(input_path):
    df = pd.read_csv(input_path)

    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-5EASGM8\\SQLEXPRESS;'
        'DATABASE=WEATHER;' 
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    cursor.execute("""
        IF OBJECT_ID('WEATHERDATA', 'U') IS NOT NULL DROP TABLE WEATHERDATA;
        CREATE TABLE WEATHERDATA (
            City NVARCHAR(50) NOT NULL, 
            Description NVARCHAR(100), 
            Temperature_F FLOAT, 
            Feels_Like_F FLOAT, 
            Minimum_Temp_F FLOAT, 
            Maximum_Temp_F FLOAT, 
            Pressure INT, 
            Humidity INT, 
            Wind_Speed FLOAT, 
            Time_of_Record DATETIME, 
            Sunrise_Local_Time DATETIME, 
            Sunset_Local_Time DATETIME
        )
    """)

    for row in df.itertuples(index=False):
        cursor.execute('''
            INSERT INTO WEATHERDATA (City, Description, Temperature_F, Feels_Like_F, 
            Minimum_Temp_F, Maximum_Temp_F, Pressure, Humidity, Wind_Speed, 
            Time_of_Record, Sunrise_Local_Time, Sunset_Local_Time) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ''',
        (
            row.City,
            row.Description,
            row.Temperature_F,
            row.Feels_Like_F,
            row.Minimum_Temp_F,
            row.Maximum_Temp_F,
            row.Pressure,
            row.Humidity,
            row.Wind_Speed,
            row.Time_of_Record,
            row.Sunrise_Local_Time,
            row.Sunset_Local_Time
        ))

    conn.commit()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def task_extract():
    api_key = 'c2478e1a85cbd8579b12709cda477e5e'
    extract_weather_data(api_key, '/tmp/weather_data.json')

def task_transform():
    transform_weather_data('/tmp/weather_data.json', '/tmp/weather_data_transformed.csv')

def task_load():
    load_weather_data('/tmp/weather_data_transformed.csv')

extract_task = PythonOperator(
    task_id='task_extract',
    python_callable=task_extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='task_transform',
    python_callable=task_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='task_load',
    python_callable=task_load,
    dag=dag,
)

extract_task >> transform_task >> load_task
