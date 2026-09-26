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
        hook = GCSHook('google_cloud_default')
        bucket_name = 'weather-bigquery-test-bronze'

        dt = data_interval_start
        previous_hour_files = hook.list(bucket_name, match_glob=f'bronze/weather/realtime/*/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}/*.json')) #type:ignore
        
        if not previous_hour_files:
            logger.warning("No JSONs for the previous hour!")
            return

        frames = {}
        columns_filter = ["source_object", "event_time", "location_name", "location_lat", "location_lon", "location_type",
                              "ingested_at_utc", "weather_temperature", "weather_humidity", "weather_windSpeed", "weather_cloudCover",
                              "weather_precipitationProbability"]
        columns_name_map = {"data_values_temperature": "weather_temperature", "data_values_humidity":"weather_humidity",
                            "data_values_windSpeed":"weather_windSpeed", "data_values_cloudCover":"weather_cloudCover",
                            "data_values_precipitationProbability":"weather_precipitationProbability", "data_time":"event_time"}
        for json_filename in previous_hour_files:
            #TODO: I need to account for the situation when several distinct locations are read.
            #      So i guess i will dictionary for frames. JSON should be opened with GSCHook
            #      All transformations remain the same. But frames.append should be changed to frames[location].append
            #      Then `result` should also be a list of dictionaries, where key will be location and value a concatenated dataframe of the corresponding frame entry.
            #      Then loop over the `result` and save with the respective location name.
            file_bytes = hook.download(bucket_name, json_filename)
            current_json = json.load(file_bytes) #type:ignore

            #validation
            if not validate_json(current_json, json_filename, logger):
                continue

            df = pd.json_normalize(current_json, sep='_')
            df = df.rename(columns=columns_name_map)
            df["source_object"] = [json_filename]
            df["ingested_at_utc"] = [pendulum.now()]
            df = df.reindex(columns=columns_filter) #filter and fill missing columns with None 

            location = df['location_name'][0].split(',')[0] #extracts the name of the city
            df['location_name'] = location
            frames.setdefault(location, []).append(df)

        #no valid jsons
        if not frames:
            logger.error('There were no valid JSON files within the last hour')
            raise AirflowException('No valid JSONs occurred during the last hour! DAG is failed')

        #TODO: Here should be a for loop that will loop through keys in frames dict. 
        #      For every key it will concatenate all the frames in the corresponding value list and save it to silver bucket
        result = pd.concat(frames)
        result = result.drop_duplicates(subset=['location_name', 'event_time'])    

        Path('/opt/airflow/include/silver_weather').mkdir(parents=True, exist_ok=True)
        result.to_csv(f'/opt/airflow/include/silver_weather/{filename}.csv', index=False)

    transform_and_load()
                    
load_to_silver()