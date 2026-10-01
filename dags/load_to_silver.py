from airflow.sdk import dag, task, Param, TaskInstance, get_current_context, CronDataIntervalTimetable #type:ignore
from airflow.sdk.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pendulum
from pendulum import datetime
import datetime as dt
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
     schedule=CronDataIntervalTimetable('@hourly', timezone="UTC"),
     params={
         "source_path": Param(None, type=["null","string"])
     })
def weather_silver_processing():

    bucket_bronze_name = 'weather-bigquery-test-bronze'
    bucket_silver_name = 'weather-bigquery-test-silver'
    load_dotenv()

    BQ_DATASET = os.getenv('BQ_DATASET')
    PROJECT_ID = os.getenv('PROJECT_ID')
    TABLE_NAME = os.getenv('TABLE_NAME')

    @task
    def transform_and_load(data_interval_start=None, ts=None):

        logger = logging.getLogger(__name__)
        hook = GCSHook('google_cloud_default')

        ctx = get_current_context()
    
        match_glob = f'bronze/weather/realtime/*/year={data_interval_start.year}/month={data_interval_start.month}/day={data_interval_start.day}/hour={data_interval_start.hour}/*.json' #type:ignore
        files_to_process = hook.list(bucket_bronze_name, match_glob=match_glob)
        #account for possible parameters
        if(ctx["params"]["source_path"]):
            files_to_process = hook.list(bucket_bronze_name, prefix=ctx["params"]["source_path"])
        
        logger.info('Interval start: %s', data_interval_start)
        if not files_to_process:
            logger.warning("No JSON files occurred within the previous hour! Or, if you provided a source path parameter, there are no JSON files with this prefix")
            raise AirflowException('No JSON files occurred within the previous hour, or there are no JSON files with the provided source path(if provided)! DAG is failed')

        frames = {}
        columns_filter = ["source_object", "event_time", "location_name", "location_lat", "location_lon", "location_type",
                              "ingested_at_utc", "weather_temperature", "weather_humidity", "weather_windSpeed", "weather_cloudCover",
                              "weather_precipitationProbability"]
        columns_name_map = {"data_values_temperature": "weather_temperature", "data_values_humidity":"weather_humidity",
                            "data_values_windSpeed":"weather_windSpeed", "data_values_cloudCover":"weather_cloudCover",
                            "data_values_precipitationProbability":"weather_precipitationProbability", "data_time":"event_time"}
        for json_filename in files_to_process:
        
            if(not json_filename.endswith('.json')):
                continue

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

            #i assumed there can be different location names per one batch of loading
            location = df['location_name'][0].split(',')[0] #extracts the name of the city
            df['location_name'] = location
            frames.setdefault(location, []).append(df)
            logger.info('Filename: %s', json_filename)

        #no valid jsons
        if not frames:
            logger.error('After validation, there are no valid JSON files within the last hour, or that have the provided source path prefix(if provided)')
            raise AirflowException('No valid JSONs after validation! DAG is failed')

        #Save separate location data into separate csv files
        created_filenames = []
        for city in frames:
            result = pd.concat(frames[city], ignore_index=True)
            result = result.drop_duplicates(subset=['location_name', 'event_time'])
            logger.info('data: %s', result)
            logger.info('date_interval_start: %s', data_interval_start)

            dt_event_time = dt.datetime.strptime(result['event_time'][0], '%Y-%m-%dT%H:%M:%SZ')
            filename = f'silver/weather/realtime/{city}/year={dt_event_time.year}/month={dt_event_time.month}/day={dt_event_time.day}/hour={dt_event_time.hour}/{ts}.csv' #type:ignore
            created_filenames.append(filename)
            hook.upload(bucket_silver_name, filename, data=result.to_csv(index=False, header=False))

        return created_filenames

    created_files = transform_and_load()

    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=bucket_silver_name,
        source_objects=created_files,
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}",
        schema_fields=[
            {"name": "source_object", "type": "STRING", "mode": "REQUIRED"},
            {"name": "event_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "location_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "location_lat", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_lon", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ingested_at_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "weather_temperature", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "weather_humidity", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "weather_windSpeed", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "weather_cloudCover", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "weather_precipitationProbability", "type": "FLOAT64", "mode": "NULLABLE"}
        ],
        write_disposition="WRITE_APPEND",
        autodetect=False
    )

    created_files>>load_csv
                    
weather_silver_processing()