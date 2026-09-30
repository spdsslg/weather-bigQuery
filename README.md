## Overview
![alt text](https://github.com/spdsslg/weather-bigQuery/blob/feature/schema_flow.png?raw=true)

The purpose of this mini project is to create an automatic pipeline that allows for weather analytics in BigQuery. An external Tommorow.io API is used to fetch the current weather data in Warsaw. Every 10 minutes a Cloud Function makes a request to the API, receives a JSON and stores it in the GCS "bronze layer" bucket. Every hour Airflow executes a DAG that validates every JSON for the last hour, flattens it and takes relevant columns. Those transformations are preformed with Pandas. Filtered data for the last hour is saved as a `.csv` file in the "silver layer" GCS bucket. The same data, after being saved to the silver layer, is saved to the BigQuery table.

It is also possible to start the DAG manually and provide a prefix for the specific files that you want to be processed

All the infrastructure is provisioned and managed with Terraform. Apart from Airflow, which runs locally in a Docker container.

Secrets Manager stores `Tommorow.io` API key. `api_key_value` is passed as a variable to the terraform commands, as well as `project_name`, `region_name` and `dataset_name`. `terraform.tfvars` can be used to automatically pass those, avoiding writing them manually every time.

IAM and Service Accounts are used to restrict access to GCP and to follow Principle of Least Privilege.

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


