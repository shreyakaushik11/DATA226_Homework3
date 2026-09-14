from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


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
    cur = return_snowflake_conn()
    target_table = "raw.weather_data"

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            latitude FLOAT,
            longitude FLOAT,
            date DATE,
            temp_max FLOAT,
            temp_min FLOAT,
            precipitation FLOAT,
            weather_code INTEGER,
            PRIMARY KEY (latitude, longitude, date)
        );
    """)

    try:
        cur.execute("BEGIN;")

        cur.execute(f"DELETE FROM {target_table}")

        for r in records:
            sql = f"""
                INSERT INTO {target_table}
                (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                VALUES (
                    {r[0]},
                    {r[1]},
                    '{r[2]}',
                    {r[3]},
                    {r[4]},
                    {r[5]},
                    {r[6]}
                )
            """
            cur.execute(sql)

        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

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