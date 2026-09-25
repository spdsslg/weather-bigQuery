resource "google_service_account" "ingest_function_sa"{
    account_id = "ingest-function-sa"
    display_name = "sa for weather fetching function"
}

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