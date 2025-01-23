terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.2.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = "modern-alpha-434617-q9"
  region      = "us-central1"
}

resource "google_storage_bucket" "bucket-de" {
  name          = var.sb_name
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset_de" {
  dataset_id = var.bd_name
}