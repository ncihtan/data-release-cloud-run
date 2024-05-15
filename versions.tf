terraform {
  required_version = ">= 0.13"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.23, < 6"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 2.1, < 4.0"
    }
  }
}