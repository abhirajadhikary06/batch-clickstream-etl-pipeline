# URL Shortener Analytics Pipeline

Batch medallion analytics pipeline for clickstream events from the FastAPI source described in [INSTRUCTION.md](INSTRUCTION.md).

## What is implemented now

### Ingestion and Bronze

- Incremental watermark tracking in [src/ingestion/watermark_store.py](src/ingestion/watermark_store.py)
- API pull with `since` watermark in [src/ingestion/fetch_events.py](src/ingestion/fetch_events.py)
- Payload validation and normalization in [src/ingestion/normalize_payload.py](src/ingestion/normalize_payload.py)
- Bronze parquet writes in [src/lakehouse/bronze_writer.py](src/lakehouse/bronze_writer.py)
- Dagster job and 30 minute schedule in [orchestration/dagster_project/jobs/clickstream_pipeline_job.py](orchestration/dagster_project/jobs/clickstream_pipeline_job.py) and [orchestration/dagster_project/schedules/every_30_min_schedule.py](orchestration/dagster_project/schedules/every_30_min_schedule.py)

### CrateDB cloud integration

- CrateDB connection smoke test in [src/warehouse/crate_connection.py](src/warehouse/crate_connection.py)
- `.env` loading for warehouse credentials in [src/warehouse/env.py](src/warehouse/env.py)
- Raw table creation and batch load helpers in [src/warehouse/raw_loader.py](src/warehouse/raw_loader.py)
- Dagster orchestration updated to validate CrateDB, load raw rows, run dbt, and finalize the watermark only after downstream success

### dbt scaffold

- dbt project config in [dbt/dbt_project.yml](dbt/dbt_project.yml)
- dbt profile in [dbt/profiles.yml](dbt/profiles.yml)
- Source declaration for raw clickstream data in [dbt/models/sources.yml](dbt/models/sources.yml)
- Staging model and tests in [dbt/models/staging/stg_clickstream_events.sql](dbt/models/staging/stg_clickstream_events.sql) and [dbt/models/staging/stg_clickstream_events.yml](dbt/models/staging/stg_clickstream_events.yml)
- Silver incremental model and tests in [dbt/models/silver/silver_clickstream_events.sql](dbt/models/silver/silver_clickstream_events.sql) and [dbt/models/silver/silver_clickstream_events.yml](dbt/models/silver/silver_clickstream_events.yml)
- Gold aggregation model and tests in [dbt/models/gold/gold_url_daily_metrics.sql](dbt/models/gold/gold_url_daily_metrics.sql) and [dbt/models/gold/gold_url_daily_metrics.yml](dbt/models/gold/gold_url_daily_metrics.yml)

## Current pipeline flow

1. Dagster pulls incremental events from the FastAPI source.
2. Events are normalized and written to Bronze parquet under `data/bronze/`.
3. The pipeline validates the CrateDB cloud connection.
4. The same batch is inserted into `raw.clickstream_events` in CrateDB.
5. dbt runs the staging, silver, and gold models.
6. dbt tests run for the touched models.
7. The watermark is advanced only after the downstream warehouse path succeeds.

## How to run and check what is implemented

All commands below assume the repository virtual environment is active and `.env` exists at the repo root.

### 1. Activate the environment

```powershell
& "c:/Abhiraj/Data Engineering/batch-clickstream-etl-pipeline/venv/Scripts/activate.ps1"
```

### 2. Start Dagster if it is not already running

```powershell
dagster dev -w orchestration/dagster_project/workspace.yaml
```

### 3. Run the pipeline job

If Dagster is already running, this executes the job against the repository code:

```powershell
dagster job execute -f orchestration/dagster_project/repository.py -d . -j clickstream_pipeline_job
```

What this checks:

- watermark lookup and update
- API fetch and payload normalization
- Bronze parquet write
- CrateDB cloud smoke test
- raw row insertion into CrateDB
- dbt staging, silver, and gold execution

### 4. Check the CrateDB cloud connection directly

```powershell
$env:CRATE_URL = 'https://crate-db-warehouse.eks1.us-west-2.aws.cratedb.net:4200'
$env:CRATE_USERNAME = 'admin'
$env:CRATE_PASSWORD = $env:CRATE_PASSWORD
& "c:/Abhiraj/Data Engineering/batch-clickstream-etl-pipeline/venv/Scripts/python.exe" -c "from src.warehouse.crate_connection import smoke_test_crate_connection; print(smoke_test_crate_connection())"
```

Expected result:

- a tuple containing the cluster name, for example `('crate-db-warehouse',)`

### 5. Check dbt project parsing and connectivity

```powershell
$env:CRATE_URL = 'https://crate-db-warehouse.eks1.us-west-2.aws.cratedb.net:4200'
$env:CRATE_USERNAME = 'admin'
$env:CRATE_PASSWORD = $env:CRATE_PASSWORD
$env:DBT_SCHEMA = 'analytics'
& "c:/Abhiraj/Data Engineering/batch-clickstream-etl-pipeline/venv/Scripts/dbt.exe" debug --project-dir dbt --profiles-dir dbt
```

What this checks:

- dbt project file syntax
- dbt profile file syntax
- native CrateDB adapter connectivity

## Known current gap

- The local sandbox in this workspace can block dbt threaded execution on Windows (`WinError 5` while creating multiprocessing pipes).
- In a normal local shell (outside this restricted execution sandbox), the same dbt commands should run against CrateDB cloud using the native `cratedb` adapter.

## Environment variables

Place these in `.env` at the repository root:

- `CRATE_URL`
- `CRATE_USERNAME`
- `CRATE_PASSWORD`
- `CRATE_VERIFY_SSL_CERT` optional, defaults to `true`
- `DBT_SCHEMA`

## Notes

- The Bronze layer remains parquet-based in `data/bronze/`.
- Dagster is the orchestration entry point and already schedules the ingestion job.
- CrateDB is treated as the cloud warehouse layer.
