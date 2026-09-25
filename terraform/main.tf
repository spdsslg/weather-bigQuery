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

