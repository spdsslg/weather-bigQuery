locals {
    project_name = get_env("GOOGLE_WEATHER_PROJECT", "weather-bigquery-test")
}

remote_state{
    backend = "gcs"

    generate = {
        if_exists = "overwrite"
        path = "backend.tf"
    }

    config = {
        project = local.project_name
        bucket = "${local.project_name}-terraform-state"
        location = "europe-west1"
        prefix = "terraform/state"
    }
}

#terragrunt wrapper passes data to terraform
inputs = {
    source_dir = "${get_original_terragrunt_dir()}/../src"
    secrets_dir = "${get_original_terragrunt_dir()}/../secrets"
}