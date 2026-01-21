# BigQuery Environment Discovery & Inventory

This module provides tools and workflows to discover, inventory, and analyze BigQuery usage across your Google Cloud Organization. It centralizes metadata about jobs, slots, and reservations to enable cost optimization and performance analysis.

## Components

### 1. BigQuery Inventory Workflow (`bq-inventory-workflow`)
A Google Cloud Workflow that automates the daily export of `INFORMATION_SCHEMA.JOBS` from all projects and regions into a centralized BigQuery table.

*   **Goal:** Create a unified history of all BigQuery jobs across the organization.
*   **Mechanism:**
    *   Iterates through all projects in the Organization.
    *   Iterates through all Google Cloud Regions (US, EU, us-central1, europe-west1, etc.).
    *   Exports the last 24 hours of job history to `antoniopaulino-billing.bq_inventory.jobs_all_projects`.
    *   Handles errors gracefully (e.g., if a project has no dataset in a specific region).

### 2. BigQuery Exporter (`bq_exporter.py`)
A Python utility to manually export BigQuery `INFORMATION_SCHEMA` views to a GCS bucket or local files. useful for ad-hoc snapshots or backup.

### 3. Slot Analyzer (`bq_slot_analyzer.py`)
A Python tool to analyze slot usage and reservation utilization.

## Setup & Deployment

### Prerequisites
*   Google Cloud Project (e.g., `antoniopaulino-billing`)
*   Service Account with permissions:
    *   `BigQuery Admin` or `Resource Viewer` (Organization Level)
    *   `BigQuery Data Editor` (Destination Project)
    *   `Workflows Invoker`

### Deploying the Workflow
The workflow is defined in `bq_inventory_workflow.yaml`. Deploy it using the `gcloud` CLI:

```bash
gcloud workflows deploy bq-inventory-workflow \
    --source=bq_inventory_workflow.yaml \
    --location=us-central1 \
    --project=antoniopaulino-billing
```

### Scheduling
The workflow is triggered by a Cloud Scheduler job (e.g., `bq-inventory-trigger`) that runs daily.

## Data Schema
The centralized table `antoniopaulino-billing.bq_inventory.jobs_all_projects` matches the schema of `INFORMATION_SCHEMA.JOBS`.

## Known Limitations
*   **Self-Referential Jobs:** Jobs that query or write to the destination inventory table itself are currently excluded from the sync to prevent recursion or locking issues.