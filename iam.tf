data "google_project" "project" {
  project_id = var.project_id
}

resource "google_service_account" "sa" {
  project = var.project_id
  account_id = var.account_id
  display_name = "Service Account used by Cloud Run Job to run data release validation"
}

resource "google_project_iam_member" "sa_bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "sa_bigquery_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "sa_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "sa_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

