# BigQuery Metadata & Usage Exporter

This script exports schemas, configuration, and usage statistics for BigQuery datasets and tables. It supports filtering out linked/external datasets and provides fallback mechanisms for permissions issues.

## Features

- Exports dataset configurations and table schemas to JSON.
- Exports storage usage (rows, logical/physical bytes) to CSV.
- Exports query history (last N days) to CSV.
- **Automatic Compression**: Archives the output directory into a ZIP file by default.
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
- `--days`: Number of days for query history (default: 7).
- `--exclude_user`: Email of user to exclude from query stats.
- `--output_dir`: Directory to save results (default: `bq_export_results`).
- `--no-compress`: Disable automatic compression of the results directory.

### Examples

**Recommended: Export Everything (Default)**
This exports configuration, storage stats, and query history for the last 7 days, and automatically compresses the results into a ZIP file.
```bash
python bq_exporter.py --project_id my-project
```

**Export with Extended History**
> **Warning:** Exporting query history for long periods (e.g., 30+ days) can result in very large CSV files and longer execution times. We recommend sticking to the default (7 days) or testing with a smaller range first.
```bash
python bq_exporter.py --project_id my-project --days 30
```

**Export Only Storage Stats (No Compression)**
Useful for quick storage audits where you need to inspect the CSV immediately without unzipping.
```bash
python bq_exporter.py --mode storage --no-compress
```
