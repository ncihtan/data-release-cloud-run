project_id = "htan-dcc"
region = "us-east1"
image_url = "ghcr.io/ncihtan/data-release-cloud-run:latest"
secret_id = "synapse_service_pat" 

# service account variables
google_service_account = {
  sa = {
    email = "data-release@htan-dcc.iam.gserviceaccount.com"
  }
}
account_id = "data-release"

# job variables
cloud_run_name = "data-release-validation"
job_name =  "data-release-validation-trigger"
job_description = "Runs validation on unreleased data"
job_schedule = "0 4 * * *"
time_zone = "America/New_York"
