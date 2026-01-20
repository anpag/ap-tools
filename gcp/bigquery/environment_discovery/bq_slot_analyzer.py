import csv
import os
import datetime
from google.cloud import bigquery
from google.cloud import resourcemanager_v3

def get_active_projects(parent_id):
    """Searches for all active projects under a given organization or folder recursively."""
    client = resourcemanager_v3.ProjectsClient()
    # Search uses a query string, e.g., 'parent:organizations/123'
    query = f"parent:{parent_id} state:ACTIVE"
    request = resourcemanager_v3.SearchProjectsRequest(query=query)
    projects = []
    for project in client.search_projects(request=request):
        projects.append({
            "project_id": project.project_id,
            "labels": project.labels
        })
    return projects

def analyze_slots(project_id, labels, regions=['region-us', 'region-eu']):
    """Analyzes slot usage for a project across specified regions."""
    client = bigquery.Client(project=project_id)
    results = []
    
    # Common regions to check if not specified
    # In a real-world scenario, we could try to discover these via dataset locations
    
    query_template = """
    SELECT 
        TIMESTAMP_TRUNC(creation_time, HOUR) as hour,
        SUM(total_slot_ms) / (1000 * 60 * 60) as avg_slots_per_hour,
        MAX(total_slot_ms) / 1000 as max_slot_seconds_single_job
    FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY 1
    HAVING avg_slots_per_hour > 0.1
    """

    for region in regions:
        try:
            query = query_template.format(region=region)
            query_job = client.query(query)
            rows = query_job.result()
            
            for row in rows:
                res = {
                    "project_id": project_id,
                    "region": region,
                    "hour": row.hour.isoformat(),
                    "avg_slots": round(row.avg_slots_per_hour, 2),
                    "max_slot_sec": round(row.max_slot_seconds_single_job, 2)
                }
                # Flatten labels into the result
                for k, v in labels.items():
                    res[f"label_{k}"] = v
                results.append(res)
        except Exception:
            # Silently skip regions where INFORMATION_SCHEMA is not accessible or no usage
            continue
            
    return results

def main(parent_id, output_file="slot_usage_report.csv"):
    print(f"Starting Slot Analysis for Parent: {parent_id}")
    projects = get_active_projects(parent_id)
    print(f"Found {len(projects)} active projects.")
    
    all_usage = []
    for p in projects:
        print(f"  Analyzing {p['project_id']}...")
        usage = analyze_slots(p['project_id'], p['labels'])
        all_usage.extend(usage)
    
    if not all_usage:
        print("No significant slot usage found in the last 30 days.")
        return

    # Extract all possible fieldnames (different projects might have different labels)
    fieldnames = set()
    for item in all_usage:
        fieldnames.update(item.keys())
    
    # Sort fieldnames to keep project_id, region, hour at the start
    base_fields = ["project_id", "region", "hour", "avg_slots", "max_slot_sec"]
    label_fields = sorted([f for f in fieldnames if f.startswith("label_")])
    final_fields = base_fields + label_fields

    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=final_fields)
        writer.writeheader()
        writer.writerows(all_usage)
    
    print(f"Success! Report saved to {output_file}")

if __name__ == "__main__":
    # Example: organizations/12345678 or folders/12345678
    # Using your Organization ID discovered earlier
    ORG_ID = "organizations/672970166928"
    main(ORG_ID)
