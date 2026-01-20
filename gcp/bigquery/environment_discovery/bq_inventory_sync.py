import argparse
import concurrent.futures
from google.cloud import bigquery
from google.cloud import resourcemanager_v3

DEST_TABLE = "antoniopaulino-billing.bq_inventory.jobs_all_projects"

def get_active_projects(parent_id):
    """Searches for all active projects recursively."""
    client = resourcemanager_v3.ProjectsClient()
    query = f"parent:{parent_id} state:ACTIVE"
    request = resourcemanager_v3.SearchProjectsRequest(query=query)
    projects = [p.project_id for p in client.search_projects(request=request)]
    
    # Fallback for testing
    known_projects = [
        "pearson-464716", "kf-test-461311", "mypinpad-dataflow", 
        "train-hackathon-2025", "bet365-dataform", "bioctx-demos", 
        "dfra-demo", "brl-demos", "blv-demos", "superdr", "antoniopaulino-billing"
    ]
    for k_id in known_projects:
        if k_id not in projects:
            projects.append(k_id)
    return projects

def sync_project_jobs(project_id, lookback_days=1):
    """Copies INFORMATION_SCHEMA.JOBS from a project to the central table as-is."""
    client = bigquery.Client(project=project_id)
    
    # Note: We use INSERT INTO to keep the schema exactly as it is in the source.
    # We filter by creation_time to allow for incremental daily runs.
    query = f"""
    INSERT INTO `{DEST_TABLE}`
    SELECT * 
    FROM `{project_id}.region-us`.INFORMATION_SCHEMA.JOBS
    WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
    """
    
    try:
        query_job = client.query(query)
        query_job.result() # Wait for completion
        print(f"  [Done] {project_id}: Synced jobs.")
        return True
    except Exception as e:
        if "Not found" in str(e) or "Access Denied" in str(e):
            # Often projects might not have BQ enabled or permissions are missing
            pass
        else:
            print(f"  [Error] {project_id}: {e}")
        return False

def main(parent_id, concurrency=10, lookback=1):
    print(f"Starting Inventory Sync to {DEST_TABLE}")
    projects = get_active_projects(parent_id)
    print(f"Found {len(projects)} projects to check.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(sync_project_jobs, pid, lookback): pid for pid in projects}
        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("\nSync complete.")

import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync BigQuery job history from all projects to a central table.")
    parser.add_argument("parent_id", nargs="?", help="The parent resource ID (e.g., organizations/12345)")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of parallel syncs.")
    parser.add_argument("--lookback", type=int, default=1, help="Number of days to sync (Default: 1).")
    
    args = parser.parse_args()

    # Fallback to Environment Variables (Cloud Run friendly)
    parent_id = args.parent_id or os.environ.get("PARENT_ID")
    if not parent_id:
        raise ValueError("PARENT_ID must be provided via argument or environment variable.")
        
    # Allow Env Vars to override defaults if args not explicitly set (simplified logic)
    concurrency = args.concurrency if args.concurrency != 10 else int(os.environ.get("CONCURRENCY", 10))
    lookback = args.lookback if args.lookback != 1 else int(os.environ.get("LOOKBACK", 1))

    main(parent_id, concurrency, lookback)
