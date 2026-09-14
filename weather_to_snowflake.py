from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import requests


@task
def extract_weather(latitude, longitude):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ]
    }

    response = requests.get(url, params=params)
    data = response.json()

    records = []

    for i in range(len(data["daily"]["time"])):
        records.append([
            latitude,
            longitude,
            data["daily"]["time"][i],
            data["daily"]["temperature_2m_max"][i],
            data["daily"]["temperature_2m_min"][i],
            data["daily"]["precipitation_sum"][i],
            data["daily"]["weather_code"][i]
        ])

    return records


@task
def load_weather(records):
    print("Number of weather records:", len(records))

    # Snowflake loading code will be added later
    # when we do the Snowflake Connection requirement


with DAG(
    dag_id="WeatherToSnowflake",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["ETL"],
    schedule="30 2 * * *"
) as dag:

    latitude = float(Variable.get("latitude"))
    longitude = float(Variable.get("longitude"))

    weather_data = extract_weather(latitude, longitude)

    load_weather(weather_data)