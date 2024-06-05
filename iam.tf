data "google_project" "project" {
  project_id = var.project_id
}

data "google_service_account" "existing_sa" {
  project    = var.project_id
  account_id = var.account_id
  depends_on = [data.google_project.project]
}

resource "google_service_account" "sa" {
  project      = var.project_id
  account_id   = var.account_id
  display_name = "Service Account used by Cloud Run Job to run data release validation"

  # Create only if the service account does not already exist
  count = length(data.google_service_account.existing_sa.email) == 0 ? 1 : 0
}

resource "google_project_iam_member" "sa_bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${var.account_id}@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_bigquery_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${var.account_id}@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.account_id}@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "sa_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${var.account_id}@${var.project_id}.iam.gserviceaccount.com"
}