# data-release-cloud-run
Set up a Google Cloud Run job to validate unreleased data files.

Scheduled to run daily at 0400 ET.

Results from the validation scripts are stored in the `htan-dcc.data_release` Google BigQuery dataset. These tables serve as the foundation for generating the final list of releasable HTAN portal files.

## Requirements
Requires access to deploy resources in the HTAN Google Cloud Project, `htan-dcc`. Please contact an owner of `htan-dcc` to request access (Owners in 2024: Clarisse Lau, Vesteinn Thorsson, William Longabaugh, ISB)

## Prerequisites
- Create a [Synapse Auth Token](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens) secret in [Secret Manager](https://cloud.google.com/secret-manager/docs). Requires download access to all individual HTAN-center Synapse projects. Currently uses `synapse-service-HTAN-lambda` service account. 

- Install [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) >= 1.7.0

## Docker Image
Before creating job, build and push a docker image to Google Artifact Registry (recommended)

```
cd src
docker build . -t us-docker.pkg.dev/<gc-project>/gcr.io/<image-name>
docker push us-docker.pkg.dev/<gc-project>/gcr.io/<image-name>
```

## Deploy Cloud Resources
Define variables in [terraform.tfvars](https://github.com/ncihtan/bq-metadata-cloud-run/blob/main/terraform.tfvars). Variable descriptions can be found in [variables.tf](https://github.com/ncihtan/bq-metadata-cloud-run/blob/main/variables.tf)

```
terraform init
terraform plan
terraform apply
```

## Validation Checks
The release validation scripts implement a number of file-level checks including: 

- Unique `HTAN Data File ID`
- Unique `HTAN Biospecimen ID`
- Unique `HTAN Participant ID` within demographics manifest(s)
- Compliance of `HTAN Data File ID` with HTAN ID SOP format
- Existence of Synapse ID provided in Synapse metadata
- Existence of listed Adjacent Biospecimen ID as biospecimen entity
- Presence of Parent IDs
- Minimum `DependsOn` attributes are present in metadata manifest

Files failing any of the above checks are added to error output table: `htan-dcc.data_release.errors`

#### Additional checks available for internal use, but not mandatory for release include:
- Uniqueness of base filenames
- Equivalence of a file's Synapse name, alias, and bucket basename
- Identification of non-data-model columns added to a manifest
