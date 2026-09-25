from airflow.sdk import dag, task, TaskInstance #type:ignore
from airflow.sdk.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path
import pendulum
from pendulum import datetime
import glob
import pandas as pd
import json
import logging

def validate_json(current_json, json_filename, logger):

    #structure validation
    if(not isinstance(current_json.get('data'), dict) 
       or not isinstance(current_json.get('location'), dict)
       or not isinstance(current_json.get('data',{}).get('values'), dict)): 
        logger.warning("File %s has incorrect structure", json_filename)
        return False

    values_dict = current_json['data']['values']
    #validate existing value fields types
    for col in values_dict:
        col_value = values_dict.get(col)
        if type(col_value) not in (int, float):
            return False

    #check if the fields live within the range
    valid_ranges = {"humidity":[0,100], "cloudCover":[0,100], "precipitationProbability":[0,100],
                    "temperature":[-90, 70], "windSpeed":[0,150]}
    for col in valid_ranges:
        if col in values_dict and (values_dict[col]<valid_ranges[col][0] or values_dict[col]>valid_ranges[col][1]):
            return False

    return True

@dag(start_date=datetime(2026,9,23),
     schedule='@hourly')
def load_to_silver():

    @task
    def transform_and_load(run_id=None, data_interval_start=None):
        logger = logging.getLogger(__name__)

        filename = f"weather_hourly_{run_id}"
        dt = data_interval_start

        previous_hour_files = glob.glob(f'/opt/airflow/include/weather/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}/*.json') #type:ignore
        if not previous_hour_files:
            logger.warning("No JSONs for the previous hour!")
            return

        frames = []
        columns_filter = ["source_object", "event_time", "location_name", "location_lat", "location_lon", "location_type",
                              "ingested_at_utc", "weather_temperature", "weather_humidity", "weather_windSpeed", "weather_cloudCover",
                              "weather_precipitationProbability"]
        columns_name_map = {"data_values_temperature": "weather_temperature", "data_values_humidity":"weather_humidity",
                            "data_values_windSpeed":"weather_windSpeed", "data_values_cloudCover":"weather_cloudCover",
                            "data_values_precipitationProbability":"weather_precipitationProbability", "data_time":"event_time"}
        for json_filename in previous_hour_files:
            with open(json_filename, 'r', encoding='utf-8') as json_file:
                current_json = json.load(json_file)

                #validation
                if not validate_json(current_json, json_filename, logger):
                    continue

                df = pd.json_normalize(current_json, sep='_')
                df = df.rename(columns=columns_name_map)
                df["source_object"] = [json_filename]
                df["ingested_at_utc"] = [pendulum.now()]
                df = df.reindex(columns=columns_filter) #filter and fill missing columns with None 
        
                frames.append(df)

        df = df.drop_duplicates(subset=['location_name', 'event_time'])    

        #no valid jsons
        if not frames:
            logger.error('There were no valid JSON files within the last hour')
            raise AirflowException('No valid JSONs occurred during the last hour! DAG is failed')

        result = pd.concat(frames)
        Path('/opt/airflow/include/silver_weather').mkdir(parents=True, exist_ok=True)
        result.to_csv(f'/opt/airflow/include/silver_weather/{filename}.csv', index=False)

    transform_and_load()
                    
load_to_silver()