import argparse
import json
import csv
import os
import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Forbidden

def setup_output_dir(output_dir):
    """Creates the output directory structure."""
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'config', 'datasets'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'config', 'schemas'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'storage'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'queries'), exist_ok=True)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    return str(obj)

def export_configuration(client, project_id, output_dir):
    """Exports dataset configurations and table schemas."""
    print(f"Starting Configuration Export for Project: {project_id}...")
    
    datasets = list(client.list_datasets())
    print(f"Found {len(datasets)} datasets.")

    for dataset_item in datasets:
        dataset_id = dataset_item.dataset_id
        try:
            full_dataset = client.get_dataset(dataset_item.reference)
            
            # FILTER: Exclude Linked Datasets (Analytics Hub)
            ds_api_repr = full_dataset.to_api_repr()
            if ds_api_repr.get('type') == 'LINKED':
                print(f"  Skipping Linked Dataset: {dataset_id}")
                continue

            # Export Dataset Config
            ds_config = {
                "dataset_id": full_dataset.dataset_id,
                "location": full_dataset.location,
                "description": full_dataset.description,
                "labels": full_dataset.labels,
                "created": full_dataset.created,
                "modified": full_dataset.modified,
                "default_table_expiration_ms": full_dataset.default_table_expiration_ms,
                "access_entries": [entry.to_api_repr() for entry in full_dataset.access_entries]
            }
            
            ds_filename = os.path.join(output_dir, 'config', 'datasets', f"{dataset_id}.json")
            with open(ds_filename, 'w') as f:
                json.dump(ds_config, f, default=json_serial, indent=2)
            
            # Export Table Schemas
            print(f"  Exporting schemas for dataset: {dataset_id}...")
            tables = list(client.list_tables(full_dataset))
            
            ds_schema_dir = os.path.join(output_dir, 'config', 'schemas', dataset_id)
            os.makedirs(ds_schema_dir, exist_ok=True)

            for table_item in tables:
                table_ref = table_item.reference
                try:
                    table = client.get_table(table_ref)

                    # FILTER: Skip Views and External tables (consistent with storage export)
                    if table.table_type in ['VIEW', 'EXTERNAL']:
                        continue
                    
                    schema_info = [
                        {
                            "name": field.name,
                            "type": field.field_type,
                            "mode": field.mode,
                            "description": field.description,
                            "fields": [sub.to_api_repr() for sub in field.fields] if field.fields else []
                        }
                        for field in table.schema
                    ]

                    partitioning = "NONE"
                    if table.time_partitioning:
                        partitioning = f"TIME ({table.time_partitioning.type}, field: {table.time_partitioning.field})"
                    elif table.range_partitioning:
                        partitioning = f"RANGE (field: {table.range_partitioning.field})"

                    clustering = table.clustering_fields if table.clustering_fields else "NONE"

                    table_config = {
                        "table_id": table.table_id,
                        "type": table.table_type,
                        "partitioning": partitioning,
                        "clustering": clustering,
                        "schema": schema_info
                    }

                    t_filename = os.path.join(ds_schema_dir, f"{table.table_id}.json")
                    with open(t_filename, 'w') as f:
                        json.dump(table_config, f, default=json_serial, indent=2)

                except Exception as e:
                    print(f"    Error processing table {table_item.table_id}: {e}")

        except Exception as e:
            print(f"  Error processing dataset {dataset_id}: {e}")

def export_storage_usage(client, project_id, output_dir):
    """Exports storage usage stats using INFORMATION_SCHEMA with a fallback to API iteration."""
    print(f"Starting Storage Usage Export for Project: {project_id}...")
    
    # Group datasets by region
    region_map = {} # region -> [dataset_id]
    all_datasets = []

    print("  Discovering datasets and regions...")
    try:
        # Use full_dataset to ensure we get location
        for ds_item in client.list_datasets():
            try:
                ds_ref = ds_item.reference
                dataset = client.get_dataset(ds_ref)
                
                # FILTER: Exclude Linked Datasets (Analytics Hub)
                ds_api_repr = dataset.to_api_repr()
                if ds_api_repr.get('type') == 'LINKED':
                    print(f"    Skipping Linked Dataset: {dataset.dataset_id}")
                    continue

                loc = dataset.location
                if not loc:
                    loc = "US" # Fallback
                
                if loc not in region_map:
                    region_map[loc] = []
                region_map[loc].append(dataset.dataset_id)
                all_datasets.append(dataset)
            except Exception as e:
                print(f"    Warning: Could not fetch details for dataset {ds_item.dataset_id}: {e}")
    except Exception as e:
        print(f"  Error listing datasets: {e}")
        return

    if not region_map:
        print("  No datasets found in this project.")
        return

    print(f"  Found regions: {', '.join(region_map.keys())}")

    csv_file = os.path.join(output_dir, 'storage', 'storage_usage.csv')
    
    # Write Header
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['project_id', 'dataset_id', 'table_name', 'table_type', 'region', 'total_rows', 'logical_bytes', 'physical_bytes', 'logical_gb', 'physical_gb', 'method'])

    total_tables_found = 0

    for region, datasets in region_map.items():
        print(f"  Processing region: {region} ({len(datasets)} datasets)...")
        
        # Method 1: Try INFORMATION_SCHEMA (Fast)
        method = "INFORMATION_SCHEMA"
        rows_fetched = 0
        
        try:
            # Note: TABLE_STORAGE only contains Tables and Materialized Views (no logical Views)
            query = f"""
                SELECT 
                    table_schema AS dataset_id,
                    table_name,
                    total_rows,
                    total_logical_bytes,
                    total_physical_bytes
                FROM `{project_id}.region-{region}.INFORMATION_SCHEMA.TABLE_STORAGE`
            """
            
            query_job = client.query(query)
            results = list(query_job.result()) # Consume to check length
            
            if results:
                with open(csv_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    for row in results:
                        l_gb = round(row.total_logical_bytes / (1024**3), 4) if row.total_logical_bytes else 0
                        p_gb = round(row.total_physical_bytes / (1024**3), 4) if row.total_physical_bytes else 0
                        
                        writer.writerow([
                            project_id,
                            row.dataset_id,
                            row.table_name,
                            'TABLE', # TABLE_STORAGE only lists storage-backed tables
                            region,
                            row.total_rows,
                            row.total_logical_bytes,
                            row.total_physical_bytes,
                            l_gb,
                            p_gb,
                            method
                        ])
                        rows_fetched += 1
                print(f"    Successfully exported {rows_fetched} tables using INFORMATION_SCHEMA.")
                total_tables_found += rows_fetched
                continue # Success for this region, move to next

        except Exception as e:
            print(f"    Notice: Fast scan failed for region {region} (Access Denied or Empty). Switching to granular scan...")
        
        if rows_fetched == 0:
            print(f"    Fallback: Iterating tables via API for region {region}...")
            # Method 2: Fallback to API iteration (Slow but reliable)
            method = "API_FALLBACK"
            region_tables_count = 0
            
            for ds_id in datasets:
                try:
                    ds_ref = client.dataset(ds_id)
                    tables = client.list_tables(ds_ref)
                    
                    with open(csv_file, 'a', newline='') as f:
                        writer = csv.writer(f)
                        for table_item in tables:
                            try:
                                table = client.get_table(table_item.reference)
                                
                                # FILTER: Skip Views and External tables (user requested "owned" tables)
                                if table.table_type in ['VIEW', 'EXTERNAL']:
                                    continue

                                # API provides num_bytes (logical) and num_rows. 
                                logical_bytes = table.num_bytes or 0
                                rows = table.num_rows or 0
                                physical_bytes = 0 # Not directly available in basic Table object
                                
                                l_gb = round(logical_bytes / (1024**3), 4)
                                
                                writer.writerow([
                                    project_id,
                                    ds_id,
                                    table.table_id,
                                    table.table_type,
                                    region,
                                    rows,
                                    logical_bytes,
                                    physical_bytes,
                                    l_gb,
                                    0,
                                    method
                                ])
                                region_tables_count += 1
                            except Exception as e:
                                pass # Skip table errors
                    
                except Exception as e:
                    print(f"      Error accessing dataset {ds_id}: {e}")
            
            print(f"    Exported {region_tables_count} tables via API Fallback.")
            total_tables_found += region_tables_count

    if total_tables_found == 0:
        print("  WARNING: No tables found in any region using either method.")

def export_query_usage(client, project_id, output_dir, days, exclude_user):
    """Exports query history."""
    print(f"Starting Query Usage Export (Last {days} days)...")
    
    min_creation_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
    
    jobs_iter = client.list_jobs(all_users=True, min_creation_time=min_creation_time)
    
    csv_file = os.path.join(output_dir, 'queries', f'query_history_{days}days.csv')
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['job_id', 'user_email', 'created', 'ended', 'duration_sec', 'bytes_billed', 'bytes_processed', 'cache_hit', 'error_result', 'query_snippet'])
        
        count = 0
        for job in jobs_iter:
            if job.job_type != 'query':
                continue
                
            if exclude_user and job.user_email == exclude_user:
                continue

            created = job.created
            ended = job.ended
            duration = (ended - created).total_seconds() if ended and created else 0
            
            # Safe access to stats
            bytes_billed = job.total_bytes_billed or 0
            bytes_processed = job.total_bytes_processed or 0
            cache_hit = job.cache_hit or False
            error = job.error_result['message'] if job.error_result else None
            
            # Truncate query to avoid massive CSV rows
            query_snippet = job.query[:1000].replace('\n', ' ') if job.query else ""
            
            writer.writerow([
                job.job_id,
                job.user_email,
                created.isoformat() if created else "",
                ended.isoformat() if ended else "",
                duration,
                bytes_billed,
                bytes_processed,
                cache_hit,
                error,
                query_snippet
            ])
            count += 1
            if count % 100 == 0:
                print(f"  Processed {count} queries...", end='\r')
        
    print(f"\nFinished Query Export. Total records: {count}")

def main():
    parser = argparse.ArgumentParser(description="BigQuery Metadata & Usage Exporter")
    parser.add_argument('--project_id', type=str, help='GCP Project ID', required=False)
    parser.add_argument('--output_dir', type=str, default='bq_export_results', help='Directory to save results')
    parser.add_argument('--mode', type=str, choices=['all', 'config', 'storage', 'queries'], default='all', help='Export mode')
    parser.add_argument('--days', type=int, default=7, help='Number of days for query history (default: 7)')
    parser.add_argument('--exclude_user', type=str, help='Email of user to exclude from query stats')

    args = parser.parse_args()

    # Client setup
    try:
        if args.project_id:
            client = bigquery.Client(project=args.project_id)
        else:
            client = bigquery.Client()
            print(f"Detected Project ID: {client.project}")
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        return

    project_id = client.project
    setup_output_dir(args.output_dir)

    if args.mode in ['all', 'config']:
        export_configuration(client, project_id, args.output_dir)
    
    if args.mode in ['all', 'storage']:
        export_storage_usage(client, project_id, args.output_dir)

    if args.mode in ['all', 'queries']:
        export_query_usage(client, project_id, args.output_dir, args.days, args.exclude_user)

    print(f"\nAll requested exports completed. Check directory: {args.output_dir}")

if __name__ == "__main__":
    main()
