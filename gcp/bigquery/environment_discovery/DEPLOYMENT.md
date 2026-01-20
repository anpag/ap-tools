# Deployment Guide: BigQuery Inventory Sync (Cloud Run)

This guide explains how to deploy the `bq_inventory_sync.py` script as a Serverless Cloud Run Job that runs daily to audit your organization's BigQuery usage.

## 1. Prerequisites

*   **Project:** `antoniopaulino-billing` (or your chosen admin project).
*   **APIs Enabled:** `run.googleapis.com`, `cloudbuild.googleapis.com`, `artifactregistry.googleapis.com`.
*   **Service Account:** You need a Service Account (e.g., `bq-auditor@antoniopaulino-billing.iam.gserviceaccount.com`) with:
    *   `BigQuery Data Editor` on the destination dataset (`antoniopaulino-billing`).
    *   `BigQuery Metadata Viewer` (or Resource Viewer) on the Organization/Folder level.

## 2. Build the Container Image

Navigate to the directory containing the Dockerfile:

```bash
cd gcp/bigquery/environment_discovery
```

Submit the build to Cloud Build:

```bash
gcloud builds submit --tag gcr.io/antoniopaulino-billing/bq-inventory-sync:latest .
```

## 3. Create the Cloud Run Job

Create the job definition. Replace `[SERVICE_ACCOUNT_EMAIL]` with your actual SA email.

```bash
gcloud run jobs create bq-inventory-sync-job \
    --image gcr.io/antoniopaulino-billing/bq-inventory-sync:latest \
    --region us-central1 \
    --set-env-vars PARENT_ID="organizations/672970166928" \
    --service-account [SERVICE_ACCOUNT_EMAIL]
```

## 4. Schedule Daily Execution

Use Cloud Scheduler to trigger the job every day at midnight.

```bash
gcloud scheduler jobs create http bq-daily-sync \
    --location us-central1 \
    --schedule "0 0 * * *" \
    --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/antoniopaulino-billing/jobs/bq-inventory-sync-job:run" \
    --http-method POST \
    --oauth-service-account-email [SERVICE_ACCOUNT_EMAIL]
```

## 5. Verification

You can manually trigger the job to test it:

```bash
gcloud run jobs execute bq-inventory-sync-job --region us-central1
```

Check the logs in the Cloud Console to verify it successfully discovered projects and inserted rows.

```