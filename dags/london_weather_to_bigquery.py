from airflow.decorators import dag, task

from datetime import datetime, timedelta


@task(retries=3, retry_delay=timedelta(seconds=30))
def fetch_time():
    import requests

    return requests.get('http://worldtimeapi.org/api/timezone/Europe/London').json()


@task.virtualenv(requirements=['openmeteo-requests'], venv_cache_path='/tmp/venv_cache', retries=3, retry_delay=timedelta(seconds=30))
def fetch_weather():
    import openmeteo_requests

    # Setup the Open-Meteo API client
    openmeteo = openmeteo_requests.Client()

    # Get current weather in London
    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude': 53.4808,
        'longitude': 2.2426,
        'current': ['temperature_2m', 'apparent_temperature', 'is_day', 'precipitation', 'wind_speed_10m', 'wind_direction_10m']
    }
    responses = openmeteo.weather_api(url, params=params)
    current_weather = responses[0].Current()

    # Current values. The order of variables needs to be the same as requested.
    return {
        'lat_long': f"{params['latitude']}, {params['longitude']}",
        'temperature_2m': current_weather.Variables(0).Value(),
        'apparent_temperature': current_weather.Variables(1).Value(),
        'is_day': current_weather.Variables(2).Value(),
        'precipitation': current_weather.Variables(3).Value(),
        'wind_speed_10m': current_weather.Variables(4).Value(),
        'wind_direction_10m': current_weather.Variables(5).Value()
    }


@task.virtualenv(requirements=['bigframes'], venv_cache_path='/tmp/venv_cache', retries=3, retry_delay=timedelta(seconds=30))
def insert_weather_to_bigquery(time, weather):
    from airflow.models import Variable

    import bigframes.pandas as bpd

    bpd.options.bigquery.project = Variable.get('gcp_project_id')
    bpd.options.bigquery.location = 'US'

    # Create BigFrames DataFrames
    time_df = bpd.DataFrame([time])
    weather_df = bpd.DataFrame([weather])

    merged_df = bpd.concat(
        [
            time_df[['utc_datetime', 'day_of_week', 'week_number', 'day_of_year']],
            weather_df
        ],
        axis=1
    )

    # Write the DataFrame to BigQuery
    merged_df.to_gbq(
        destination_table=Variable.get('bigquery_destination_table'),
        if_exists='append'
    )


@dag(start_date=datetime(2021, 1, 1), catchup=False, schedule='@hourly', tags=['weather', 'bigquery'])
def london_weather_to_bigquery():

    time = fetch_time()
    weather = fetch_weather()
    insert_weather_to_bigquery(time, weather)


london_weather_to_bigquery()
