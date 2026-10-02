variable "project_name"{
    type = string
    description = "Google cloud project name"
}

variable "region_name"{
    type = string
    description = "Region for google resources"
}

variable "dataset_name"{
    type = string
    description = "Name of the BigQuery gold layer dataset"
}

variable "api_key_value"{
    type = string
    description = "API key that will be placed to the secret manager"
}

variable "source_dir"{
    type = string
    description = "The source path to the folder with cloud function"
}

variable "secrets_dir"{
    type = string
    description = "The soucre path to the folder with authentication json that will be mounted as secrets to Docker container"
}
