# BigQuery Metadata & Usage Exporter

This script exports schemas, configuration, and usage statistics for BigQuery datasets and tables. It supports filtering out linked/external datasets and provides fallback mechanisms for permissions issues.

## Features

- Exports dataset configurations and table schemas to JSON.
- Exports storage usage (rows, logical/physical bytes) to CSV.
- Exports query history (last N days) to CSV.
- Automatically handles permission errors by falling back to slower API iteration.
- Filters out "Linked" datasets (Analytics Hub) and Views from storage reports.

## Setup

1. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```

2. Authenticate with Google Cloud:
   ```bash
   gcloud auth application-default login
   ```

## Usage

Run the script from the command line:

```bash
python bq_exporter.py --project_id YOUR_PROJECT_ID [OPTIONS]
```

### Options

- `--mode`: Select export mode. Choices: `all` (default), `config`, `storage`, `queries`.
- `--days`: Number of days for query history (default: 30).
- `--exclude_user`: Email of user to exclude from query stats.
- `--output_dir`: Directory to save results (default: `bq_export_results`).

### Examples

Export everything:
```bash
python bq_exporter.py --project_id my-project
```

Export only storage stats:
```bash
python bq_exporter.py --mode storage
```
