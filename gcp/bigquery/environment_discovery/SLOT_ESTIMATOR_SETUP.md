# BigQuery Slot Estimator: Setup & Troubleshooting Guide

This document captures the key learnings, prerequisites, and troubleshooting steps for setting up the BigQuery Slot Estimator, based on the `antoniopaulino-billing` project configuration.

## 1. Prerequisites

### APIs
The following APIs must be enabled on the project where you want to view recommendations:
*   `recommender.googleapis.com` (Recommender API)
*   `bigqueryreservation.googleapis.com` (BigQuery Reservation API)

**Command to enable:**
```bash
gcloud services enable recommender.googleapis.com bigqueryreservation.googleapis.com --project [PROJECT_ID]
```

### Permissions
The user viewing the recommendations needs specific IAM roles.
*   **Project Level:** `roles/recommender.bigQueryCapacityCommitmentsProjectViewer` or `roles/recommender.bigQueryCapacityCommitmentsViewer`.
*   **Organization Level:** Note that `roles/recommender.bigQueryCapacityCommitmentsViewer` is **NOT** supported at the Organization level (as of Jan 2026). You must grant it at the Project or Folder level.

**Common Error:** `PERMISSION_DENIED` usually means the Recommender API is disabled or the user lacks the specific viewer role.

## 2. Data Requirements

The Slot Estimator is not real-time. It requires historical data to build a prediction model.

*   **Minimum History:** Typically requires **30 days** of consistent usage data to generate a high-confidence recommendation.
*   **Data Latency:** After generating new load (e.g., for a demo), it can take **24-48 hours** for the recommender to process the data and update the UI.
*   **"Not Enough Data" Warning:** This specific error in the UI means the project's slot consumption is too low or too sporadic to model.
    *   *Threshold:* Use `INFORMATION_SCHEMA.JOBS` to verify you have significant `total_slot_ms`. A few seconds of usage is insufficient.

## 3. Load Generation for Demos

If you are setting up a fresh demo environment, you must artificially generate load to "wake up" the estimator.

### Strategy
Running simple `SELECT *` queries often hits the **Query Cache** and consumes 0 slots.
1.  **Disable Cache:** Ensure `use_query_cache=False` in your job configuration.
2.  **Use Heavy Operations:** `CROSS JOIN`, `REGEXP_CONTAINS`, or aggregations on large strings force slot consumption.
3.  **Use Large Datasets:** `bigquery-public-data.stackoverflow` or `wikipedia` are excellent sources.

### Example Script
A Python script (`bq_load_generator.py`) was created to simulate this load:
```python
# ... snippet ...
job_config = bigquery.QueryJobConfig(use_query_cache=False)
heavy_query = """
    SELECT REGEXP_CONTAINS(title, r'(?i)python|java') ...
    FROM `bigquery-public-data.stackoverflow.posts_questions` ...
"""
```

## 4. Verification

To verify your environment is actually generating data for the estimator, query the Information Schema:

```sql
SELECT 
    COUNT(*) as job_count, 
    SUM(total_slot_ms) as total_slot_ms 
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT 
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
```
If `total_slot_ms` is low (< 10,000 ms), the estimator will likely remain silent.

## 5. Troubleshooting: "Fail to resolve resource"

When using the CLI to fetch recommendations for a new project, you may see this error:

```
ERROR: (gcloud.recommender.recommendations.list) INVALID_ARGUMENT: 
detail: Fail to resolve resource 'projects/.../recommenders/google.bigquery.capacityCommitment.Recommender'
```

**Cause:** The BigQuery Slot Recommender endpoint is **dynamic**. Google Cloud only instantiates the recommender resource for a project once it detects sufficient historical usage to even *attempt* a calculation.

**Resolution:**
1.  **Do not panic.** This confirms the project is in a "Cold Start" state.
2.  **Generate Load:** Run the load generator script.
3.  **Wait:** Wait 24-48 hours for the backend to register the usage and instantiate the endpoint.
4.  **Retry:** Once the endpoint is active, the command will return either a list of recommendations or an empty list (instead of an error).

## 6. Finding Candidate Projects via Billing

If you manage many projects and don't know which one has enough usage to show a recommendation, use the **Billing Export** (if enabled) to find the top consumers of BigQuery *Compute*.

**Why:** A project with high cost might just be storing data (Storage SKU). The Slot Estimator requires **Analysis** (Compute) usage.

**Query:**
Run this in your Billing/Administration project:

```sql
SELECT 
    project.id, 
    SUM(cost) as compute_cost, 
    SUM(usage.amount) as scanned_bytes, 
    MAX(sku.description) as example_sku 
FROM `[BILLING_PROJECT].detailed_cost.gcp_billing_export_v1_...` 
WHERE service.description = 'BigQuery' 
  AND sku.description LIKE '%Analysis%'  -- Filter for Compute only
  AND usage_start_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 5
```
*   **Result:** The top project from this list is your best candidate for the Slot Estimator demo.
*   **Action:** Enable the `recommender.googleapis.com` API on that specific project.
