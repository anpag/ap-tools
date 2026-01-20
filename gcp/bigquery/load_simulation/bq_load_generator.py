import time
from google.cloud import bigquery

def run_heavy_queries(project_id, iterations=5):
    client = bigquery.Client(project=project_id)
    
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    
    # Heavy Regex on StackOverflow Questions (scans ~30GB per run)
    heavy_query = """
    SELECT 
        REGEXP_CONTAINS(title, r'(?i)python|java|c\+\+') as is_popular_lang,
        COUNT(*) as count
    FROM `bigquery-public-data.stackoverflow.posts_questions`
    WHERE creation_date > '2020-01-01'
    GROUP BY 1
    """

    for i in range(iterations):
        print(f"Starting iteration {i+1}/{iterations}...")
        try:
            job = client.query(heavy_query, job_config=job_config)
            print("  Query submitted, waiting for result (this may take 10-20s)...")
            result = job.result()
            print(f"  Iteration {i+1}: Heavy query consumed {job.slot_millis} slot-ms")
            
        except Exception as e:
            print(f"  Error in iteration {i+1}: {e}")
        
        time.sleep(1)

if __name__ == "__main__":
    PROJECT_ID = "antoniopaulino-billing"
    run_heavy_queries(PROJECT_ID, iterations=5)
