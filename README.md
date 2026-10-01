## Overview
![alt text](https://github.com/spdsslg/weather-bigQuery/blob/feature/schema_flow.png?raw=true)

The purpose of this mini project is to automate the collection and processing of Warsaw weather data, making it available in BigQuery for analysis over time. It uses Tomorrow.io API to retrieve current weather data.

Every 10 minutes, a Cloud Function requests the latest weather data and stores the raw JSON response in the Google Cloud Storage (GCS) bronze bucket. Every hour, an Airflow DAG processes the previous hour's files: it validates the data, flattens the JSON structure, and selects the relevant columns using Pandas. The processed data is saved as CSV files in the GCS silver bucket, then loaded into a BigQuery table.

The DAG can also be triggered manually with a source prefix to select specific files for processing.

**The cloud infrastructure is provisioned and managed with Terraform. Airflow runs locally in Docker containers.**

Secret Manager stores the Tomorrow.io API key. Terraform accepts the key through the `api_key_value` variable, alongside `project_name`, `region_name`, and `dataset_name`. These values can be supplied through `terraform.tfvars` instead of entering them with each Terraform command.

IAM roles and service accounts control access to Google Cloud resources, following the principle of least privilege.

## How to run

First, the infrastructure should be created. 
```Terraform
cd terraform
terraform init
terraform plan
#check if the plan is correct
terraform apply
```

After it, you need to start Airflow using 
```
docker compose up
```

Then in browser open `http://localhost:8080/` and check `weather_silver_processing` DAG.

## Silver and Gold layer Schema:

| Column name | Data type | Type | Comment |
| --- | --- | --- | --- |
| `source_object` | string | required | Source path of the file the object came from. |
| `event_time` | timestamp | required | Value from the `time` field. |
| `location_name` | string | required | |
| `location_lat` | float | optional | |
| `location_lon` | float | optional | |
| `location_type` | string | optional | |
| `ingested_at_utc` | timestamp | required | Timestamp when the rows were processed. |
| `weather_temperature` | float | optional | |
| `weather_humidity` | float | optional | |
| `weather_windSpeed` | float | optional | |
| `weather_cloudCover` | float | optional | |
| `weather_precipitationProbability` | float | optional | |

