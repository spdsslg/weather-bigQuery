import os
from dotenv import load_dotenv
import requests
import json
from pathlib import Path

load_dotenv()

api_key = os.getenv('API_KEY')

url = f"https://api.tomorrow.io/v4/weather/realtime?location=warsaw&apikey={api_key}"

headers = {
    "accept-encoding": "deflate, gzip, br",
    "accept": "application/json"
}

response = requests.get(url, headers=headers)
Path("./loaded").mkdir(parents=True, exist_ok=True)
raw_files_path = f"./loaded/weather_{response.json()['data']['time']}.json"
with open(raw_files_path, 'w', encoding='utf-8') as file:
    json.dump(response.json(), file, ensure_ascii=False, indent=4)

print(raw_files_path)