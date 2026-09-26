terraform {
    required_version = ">= 1.5.0" 
	
	required_providers { 
        google = {
            source  = "hashicorp/google"
            version = "8.4.0"
        }
    }
}

provider "google"{
    region = var.region_name
    project = var.project_name
}

resource "google_storage_bucket" "bronze_weather"{
    name = "${var.project_name}-bronze"
    location = var.region_name
    force_destroy = true #not recommended in prod
}

resource "google_storage_bucket" "silver_weather"{
    name = "${var.project_name}-silver"
    location = var.region_name
    force_destroy = true
}

resource "google_bigquery_dataset" "weather_gold_dataset"{
    dataset_id = var.dataset_name
    location = var.region_name
}

#bucket for cloud function
resource "google_storage_bucket" "function_bucket" {
    name = "${var.project_name}-function"
    location = var.region_name
    force_destroy = true
}

data "archive_file" "archive" {
    type = "zip"
    source_dir = "../src/"
    output_path = "./tmp/function.zip"
}

resource "google_storage_bucket_object" "zip" {
    source = data.archive_file.archive.output_path
    content_type = "application/zip"

    #Append to the MD5 checksum of the files's content
    #to force the zip to be updated as soon as a change occurs
    name = "src-${data.archive_file.archive.output_md5}.zip"
    bucket = google_storage_bucket.function_bucket.name
}

resource "google_cloudfunctions2_function" "function" {
    name = "weather-ingestion-function"
    location = var.region_name

    build_config {
        runtime = "python310" 
        #must match the function name in the cloud function `main.py` source code
        entry_point = "main"
        source{
            storage_source{
                #Get the source code of the cloud function as a Zip compression
                bucket = google_storage_bucket.function_bucket.name
                object = google_storage_bucket_object.zip.name
            }
        }
    }

    service_config {
        max_instance_count = 1
        available_memory = "256M"
        timeout_seconds = 60
        service_account_email = google_service_account.ingest_function_sa.email

        environment_variables = {
            BRONZE_BUCKET_NAME = google_storage_bucket.bronze_weather.name
        }

        #insert api from secret manager as an environment variable
        secret_environment_variables {
            key = "API_KEY"
            project_id = var.project_name
            secret = google_secret_manager_secret.weather_api_key.secret_id
            version = "latest"
        }
    }
}





