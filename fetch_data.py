import os
from dotenv import load_dotenv
import requests
import json
from pathlib import Path
import datetime

load_dotenv()

api_key = os.getenv('API_KEY')

url = f"https://api.tomorrow.io/v4/weather/realtime?location=warsaw&apikey={api_key}"

headers = {
    "accept-encoding": "deflate, gzip, br",
    "accept": "application/json"
}

response = requests.get(url, headers=headers)
dt = datetime.datetime.strptime(response.json()['data']['time'], '%Y-%m-%dT%H:%M:%SZ')
raw_file_path = f"./include/weather/year={dt.year}/month={dt.month}/day={dt.day}/hour={dt.hour}"
Path(raw_file_path).mkdir(parents=True, exist_ok=True)
with open(f"{raw_file_path}/file_{dt.minute}minute.json", 'w', encoding='utf-8') as file:
    json.dump(response.json(), file, ensure_ascii=False, indent=4)

print(raw_file_path)