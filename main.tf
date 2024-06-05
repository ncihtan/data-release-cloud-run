provider "google" {
    project = var.project_id
    region = var.region
}

data "google_compute_default_service_account" "default" {
}


resource "google_cloud_run_v2_job" "default" {
  name     = var.cloud_run_name
  location = var.region

  template {
    template {
      containers {
        image = var.image_url
          resources {
            limits = {
              memory = "4Gi"
              cpu    = 2
            }
           }
           env {
             name = "SYNAPSE_AUTH_TOKEN"
             value_source {
               secret_key_ref {
                 secret  = var.secret_id
                 version = "latest"
                }
              }
            }
        }
        timeout = "2700s"
        service_account = coalesce(
          length(google_service_account.sa) > 0 ? google_service_account.sa[0].email : null,
          data.google_service_account.existing_sa.email
        )
    }
  }
  lifecycle {
    ignore_changes = [
      launch_stage,
    ]
  }
  depends_on = [resource.google_service_account.sa]
}


resource "google_cloud_scheduler_job" "job" {
  name             = var.job_name
  description      = var.job_description
  schedule         = var.job_schedule
  time_zone        = var.time_zone
  attempt_deadline = "320s"
  region           = var.region

  retry_config {
    retry_count = 3
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.default.name}:run"
    oauth_token {
      service_account_email = data.google_compute_default_service_account.default.email
    }
  }

  depends_on = [resource.google_cloud_run_v2_job.default]
}
