import os
from google.cloud import storage
import requests
import json
import datetime

def main(request):

    API_KEY = os.getenv('API_KEY')
    BRONZE_BUCKET_NAME = os.getenv('BRONZE_BUCKET_NAME')

    url = f"https://api.tomorrow.io/v4/weather/realtime?location=warsaw&apikey={API_KEY}"

    headers = {
        "accept-encoding": "deflate, gzip, br",
        "accept": "application/json"
    }

    storage_client = storage.Client()

    response = requests.get(url, headers=headers)
    dt = datetime.datetime.strptime(response.json()['data']['time'], '%Y-%m-%dT%H:%M:%SZ')
    #bronze/weather/realtime/<location>/YYYY/MM/DD/HH/<timestamp>.json
    raw_file_path = f"bronze/weather/realtime/{response.json()['location']['name'].split(',')[0]}/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}/file_{dt.minute}minute.json"
    bucket = storage_client.bucket(BRONZE_BUCKET_NAME)
    blob = bucket.blob(raw_file_path)
    blob.upload_from_string(json.dumps(response.json(), ensure_ascii=False, indent=4))

    print(raw_file_path)
    return "Ingested to bronze layer successfully",200
