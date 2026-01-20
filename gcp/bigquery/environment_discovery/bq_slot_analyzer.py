import csv
import os
import argparse
import datetime
import concurrent.futures
from google.cloud import bigquery
from google.cloud import resourcemanager_v3

def get_active_projects(parent_id):
    """Searches for all active projects recursively."""
    client = resourcemanager_v3.ProjectsClient()
    query = f"parent:{parent_id} state:ACTIVE"
    request = resourcemanager_v3.SearchProjectsRequest(query=query)
    projects = []
    
    # Discovery from API
    try:
        for project in client.search_projects(request=request):
            projects.append({
                "project_id": project.project_id,
                "labels": project.labels
            })
    except Exception as e:
        print(f"Warning: Error searching projects: {e}")
    
    # Fallback for testing: Add projects discovered via billing earlier
    known_projects = [
        "pearson-464716", "kf-test-461311", "mypinpad-dataflow", 
        "train-hackathon-2025", "bet365-dataform", "bioctx-demos", 
        "dfra-demo", "brl-demos", "blv-demos", "superdr", "antoniopaulino-billing"
    ]
    
    existing_ids = [p["project_id"] for p in projects]
    for k_id in known_projects:
        if k_id not in existing_ids:
            projects.append({"project_id": k_id, "labels": {}})
            
    return projects

def analyze_slots(project_id, labels, regions=['region-us', 'region-eu']):
    """Analyzes slot usage for a project across specified regions."""
    # Note: Creating a client is lightweight, but in high concurrency scenarios
    # it might be better to pass a shared client if projects share credentials.
    # However, for different projects, new clients are safer.
    client = bigquery.Client(project=project_id)
    results = []
    
    query_template = """
    SELECT 
        TIMESTAMP_TRUNC(creation_time, HOUR) as hour,
        SUM(total_slot_ms) / (1000 * 60 * 60) as avg_slots_per_hour,
        MAX(total_slot_ms) / 1000 as max_slot_seconds_single_job
    FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY 1
    HAVING avg_slots_per_hour > 0.0001
    """

    for region in regions:
        try:
            # We catch specific errors to avoid breaking the thread
            query = query_template.format(region=region)
            query_job = client.query(query)
            rows = query_job.result()
            
            for row in rows:
                res = {
                    "project_id": project_id,
                    "region": region,
                    "hour": row.hour.isoformat(),
                    "avg_slots": round(row.avg_slots_per_hour, 5),
                    "max_slot_sec": round(row.max_slot_seconds_single_job, 2),
                    "labels": "|".join([f"{k}:{v}" for k, v in labels.items()]) if labels else "no-label"
                }
                results.append(res)
        except Exception:
            # Silently skip regions where IS is invalid or API is disabled
            continue
            
    return results

def process_project_wrapper(args):
    """Wrapper function to unpack arguments for the thread executor."""
    project_data, regions = args
    try:
        # print is thread-safe in Python but output can interleave
        print(f"  [Start] Analyzing {project_data['project_id']}...")
        result = analyze_slots(project_data['project_id'], project_data['labels'], regions)
        if result:
            print(f"    [Done] {project_data['project_id']}: Found {len(result)} records.")
        else:
            print(f"    [Done] {project_data['project_id']}: No usage.")
        return result
    except Exception as e:
        print(f"    [Error] {project_data['project_id']}: {e}")
        return []

def main(parent_id, regions, output_file="slot_usage_report.csv", concurrency=5):
    print(f"Starting Slot Analysis for Parent: {parent_id}")
    print(f"Concurrency Level: {concurrency} threads")
    
    projects = get_active_projects(parent_id)
    print(f"Processing {len(projects)} projects (including billing-discovered fallbacks)...")
    
    all_usage = []
    
    # Prepare arguments for map
    # We pass a tuple (project_dict, regions_list) to the wrapper
    work_items = [(p, regions) for p in projects]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        # Map returns an iterator that yields results as they complete (or in order if map is used)
        # Using map maintains order, submit+as_completed allows processing as they finish.
        # We use map for simplicity here as we just collect all results.
        results_iterator = executor.map(process_project_wrapper, work_items)
        
        for result in results_iterator:
            all_usage.extend(result)
    
    if not all_usage:
        print("No slot usage found in the last 30 days.")
        return

    final_fields = ["project_id", "region", "hour", "avg_slots", "max_slot_sec", "labels"]

    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=final_fields)
        writer.writeheader()
        writer.writerows(all_usage)
    
    print(f"\nSuccess! Report saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recursively analyze BigQuery slot usage for all projects under an Org or Folder.")
    parser.add_argument("parent_id", help="The parent resource ID to search (e.g., organizations/12345 or folders/67890)")
    parser.add_argument("--output", default="slot_usage_report.csv", help="Output CSV file path (default: slot_usage_report.csv)")
    parser.add_argument("--regions", nargs="+", default=['region-us', 'region-eu'], help="Regions to analyze (e.g., region-us region-europe-west2). Defaults to region-us and region-eu.")
    parser.add_argument("--concurrency", type=int, default=5, help="Number of simultaneous project queries (Default: 5). Increase carefully to avoid rate limits.")
    
    args = parser.parse_args()
    
    main(args.parent_id, args.regions, args.output, args.concurrency)
