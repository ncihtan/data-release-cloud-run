variable "project_id" {
  type        = string
  description = "The ID of the Google project where the resources will be created"
}

variable "google_service_account" {
  description = "Service account information"
  type = object({
    sa = object({
      email = string
    })
  })
}

variable "account_id" {
  type        = string
  description = "account_id of Cloud Run service account"
  default     = null
}

variable "image_url" {
  type        = string
  description = "URL of image e.g. gcr.io/project/image-name:latest"
  default     = null
}

variable "cloud_run_name" {
  type        = string
  description = "Name of Cloud Run job"
  default     = null
}

variable "job_name" {
  type        = string
  description = "The name of the scheduled job to run"
  default     = null
}

variable "job_description" {
  type        = string
  description = "Additional text to describe the job"
  default     = null
}

variable "job_schedule" {
  type        = string
  description = "The job frequency, in cron syntax"
  default     = "0 2 * * *"
}

variable "time_zone" {
  type        = string
  description = "Time zone to specify for job scheduler"
  default     = "America/New_York"
}

variable "region" {
  type        = string
  description = "The region in which resources will be applied."
}

variable "secret_id" {
  type        = string
  description = "Name of secret in secret manager containing Synapse auth token"
}