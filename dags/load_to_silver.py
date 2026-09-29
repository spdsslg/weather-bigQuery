from airflow.sdk import dag, task, TaskInstance #type:ignore
from airflow.sdk.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from pathlib import Path
import pendulum
from pendulum import datetime
import pandas as pd
import json
import logging
from dotenv import load_dotenv
import os

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
def weather_silver_processing():

    bucket_bronze_name = 'weather-bigquery-test-bronze'
    bucket_silver_name = 'weather-bigquery-test-silver'
    load_dotenv()

    BQ_DATASET = os.getenv('BQ_DATASET')
    PROJECT_ID = os.getenv('PROJECT_ID')
    TABLE_NAME = os.getenv('TABLE_NAME')

    @task
    def transform_and_load(run_id=None, data_interval_start=None, ts=None):
        logger = logging.getLogger(__name__)
        hook = GCSHook('google_cloud_default')

        dt = data_interval_start
        previous_hour_files = hook.list(bucket_bronze_name, match_glob=f'bronze/weather/realtime/*/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}/*.json') #type:ignore
        
        if not previous_hour_files:
            logger.warning("No JSON files occurred within the previous hour!")
            raise AirflowException('No JSON files occurred within the previous hour! DAG is failed')

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
            file_bytes = hook.download(bucket_bronze_name, json_filename)
            current_json = json.loads(file_bytes) #type:ignore 

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
        created_filenames = []
        for city in frames:
            result = pd.concat(frames[city])
            result = result.drop_duplicates(subset=['location_name', 'event_time'])    

            filename = f'silver/weather/realtime/{city}/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}/{ts}.csv' #type:ignore
            created_filenames.append(filename)
            hook.upload(bucket_silver_name, filename, data=result.to_csv(index=False))

        return created_filenames

    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=bucket_silver_name,
        source_objects=transform_and_load(),
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}",
        schema_fields=[
            {"name": "source_object", "type": "STRING", "mode": "REQUIRED"},
            {"name": "event_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "location_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "location_lat", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "location_lon", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "location_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ingested_at_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "weather_temperature", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "weather_humidity", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "weather_windSpeed", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "weather_cloudCover", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "weather_precipitationProbability", "type": "NUMERIC", "mode": "NULLABLE"}
        ],
        write_disposition="WRITE_APPEND",
        autodetect=False
    )

    transform_and_load()>>load_csv
                    
weather_silver_processing()