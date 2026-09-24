from airflow.sdk import dag, task, TaskInstance #type:ignore
from pendulum import datetime
import glob
import pandas as pd
import json

@dag(start_date=datetime(2026,9,23),
     schedule='@hourly')
def load_to_silver():

    @task
    def transform_and_load(run_id=None, data_interval_start=None):
        filename = f"weather_hourly_{run_id}"
        dt = data_interval_start

        previous_hour_files = glob.glob(f'/opt/airflow/include/weather/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}/*.json') #type:ignore
        if not previous_hour_files:
            print("No JSONs for the previous hour!")
            return

        frames = []
        for json_filename in previous_hour_files:
            with open(json_filename, 'r') as json_file:
                current_json = json.load(json_file)

                df = pd.json_normalize(current_json, sep='_')

                frames.append(df)

        result = pd.concat(frames)
        result.to_csv(f'/opt/airflow/include/silver_weather/{filename}.csv', index=False)

    transform_and_load()
                    
load_to_silver()