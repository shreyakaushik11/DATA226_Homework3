# DATA 226 - Homework 3

## Weather ETL Pipeline with Airflow and Snowflake

This project ports Homework 2 into Apache Airflow.

The DAG retrieves the last 60 days of historical weather data for San Jose using the Open-Meteo API and loads the data into Snowflake.

## Airflow Tasks

The DAG contains two tasks:

- `extract_weather`
  - Reads weather data from the Open-Meteo Historical Weather API.

- `load_weather`
  - Connects to Snowflake using an Airflow Snowflake Connection.
  - Creates the target table if it does not exist.
  - Deletes existing records.
  - Inserts the latest 60 days of weather data.
  - Uses a SQL transaction with `BEGIN`, `COMMIT`, and `ROLLBACK`.

Task dependency:

`extract_weather -> load_weather`

## Airflow Variables

The following Airflow Variables are used:

- `latitude`
- `longitude`

## Snowflake Connection

Airflow connection ID:

`snowflake_conn`

The Snowflake connection uses key-pair authentication.

## Target Table

`RAW.WEATHER_DATA`

Columns:

- latitude
- longitude
- date
- temp_max
- temp_min
- precipitation
- weather_code

## Schedule

The DAG is scheduled using:

`30 2 * * *`

## DAG

DAG ID:

`WeatherToSnowflake`