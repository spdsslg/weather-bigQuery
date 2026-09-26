resource "google_service_account" "ingest_function_sa"{
    account_id = "ingest-function-sa"
    display_name = "sa for weather fetching function"
}

#secret creation
resource "google_secret_manager_secret" "weather_api_key"{
    secret_id = "weather-api-key"

    replication {
        auto {}
    }
}

resource "google_service_account" "airflow_sa"{
    account_id = "airflow-sa"
    display_name = "sa for Airflow in Docker"
}

resource "google_secret_manager_secret_iam_member" "secret_accessor"{
    secret_id = google_secret_manager_secret.weather_api_key.id
    role = "roles/secretmanager.secretAccessor"
    member = "serviceAccount:${google_service_account.ingest_function_sa.email}"
}

resource "google_storage_bucket_iam_member" "function_bronze_admin"{
    bucket = google_storage_bucket.bronze_weather.name
    role = "roles/storage.objectAdmin"
    member = "serviceAccount:${google_service_account.ingest_function_sa.email}"
}

resource "google_storage_bucket_iam_member" "airflow_bronze_reader"{
    bucket = google_storage_bucket.bronze_weather.name
    role = "roles/storage.objectViewer"
    member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "airflow_silver_admin"{
    bucket = google_storage_bucket.silver_weather.name
    role = "roles/storage.objectAdmin"
    member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow_bigquery_editor"{
    dataset_id = google_bigquery_dataset.weather_gold_dataset.dataset_id
    role = "roles/bigquery.dataEditor"
    member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

#put the api value to the secret manager
resource "google_secret_manager_secret_version" "weather-api-key-value"{
    secret = google_secret_manager_secret.weather_api_key.id
    secret_data = var.api_key_value
}

# #setting roles to invoke and run the cloud function
# resource "google_cloudfunctions2_function_iam_member" "invoker" {
#   project        = google_cloudfunctions2_function.function.project
#   location       = google_cloudfunctions2_function.function.location
#   cloud_function = google_cloudfunctions2_function.function.name
#   role           = "roles/cloudfunctions.invoker"
#   member         = "serviceAccount:${google_service_account.ingest_function_sa.email}"
# }

# resource "google_cloud_run_service_iam_member" "cloud_run_invoker" {
#   project = google_cloudfunctions2_function.function.project
#   location = google_cloudfunctions2_function.function.location
#   service = google_cloudfunctions2_function.function.name
#   role = "roles/run.invoker"
#   member = "serviceAccount:${google_service_account.ingest_function_sa.email}"
# }

# #cloud function invokation
# resource "google_cloud_scheduler_job" "invoke_cloud_function" {
#   name = "invoke-gcf-function"
#   description = "Schedule the HTTPS trigger for cloud function"
#   schedule = "*/10 * * * *" #every 10 mins
#   project = google_cloudfunctions2_function.function.project
#   region = google_cloudfunctions2_function.function.location

#   http_target {
#     uri         = google_cloudfunctions2_function.function.service_config[0].uri
#     http_method = "POST" #allows JSON payloads to be passed to the function; works as GET here 
#     oidc_token {
#       audience  = "${google_cloudfunctions2_function.function.service_config[0].uri}/"
#       service_account_email = google_service_account.ingest_function_sa.email
#     }
#   }
# }