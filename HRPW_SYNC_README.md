# HRPW Database Sync System

A modular R-based data pipeline that syncs HRPW data from PostgreSQL to SurveyCTO and Google Sheets with automated enumerator assignment.

## Setup

###  Configuration File

Copy the template and fill in your values:

```bash
cp params/hrpw_sync.yaml.template params/hrpw_sync.yaml
```

Edit `params/hrpw_sync.yaml`:

```yaml
output_file_url: https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID

queries_file: R/hrpw_queries.sql

database:
  dbname: your_database_name
  host: localhost
  port: 5432
  user: your_username
  password: your_password
```

#### SQL Queries File

Define your datasets in `R/hrpw_queries.sql` using this format:

**Key markers:**
- `-- @dataset_id:` Unique identifier for the dataset
- `-- @enumerators:` Comma-separated list of enumerator emails

#### SurveyCTO
Ensure your `utilities.R` contains a `get_scto_auth()` function that returns valid SurveyCTO credentials.

## How It Works

### 1. Connection Initialization
- Establishes PostgreSQL connection
- Authenticates with SurveyCTO API
- Authenticates with Google Sheets API

### 2. SQL Query Parsing
- Reads `hrpw_queries.sql`
- Extracts dataset IDs and enumerator lists
- Separates SQL queries by dataset markers

### 3. Data Processing (per dataset)
- Executes SQL query against PostgreSQL
- Fetches existing data from Google Sheets (or SurveyCTO as fallback)
- Assigns enumerators using round-robin from last assignment

### 4. Data Upload
- **SurveyCTO**: Appends new records to dataset for mobile collection
- **Google Sheets**: Creates/updates sheet with normalized data
- Writes sync status (timestamp, row count) to status sheet

### 5. Cleanup
- Closes database connections
- Logs completion status

## Contributing

When adding new datasets:
1. Add SQL query with appropriate markers to `hrpw_queries.sql`
2. List enumerator emails in `@enumerators` marker
3. Ensure query returns an `id` column for deduplication

## Crontab scheduling details

Below are the details of the crontab scheduling :

# m h  dom mon dow   command
0 6 * * * docker run r-surveycto >> /home/prod-apps/sync-source-replicas/logs/surveycto_sync-$(date +\%Y-\%m-\%d-\%H-\%M-\%S).log 2>&1

It runs at 6 AM in the morning and the logs are stored in this folder /home/prod-apps/sync-source-replicas/logs/
