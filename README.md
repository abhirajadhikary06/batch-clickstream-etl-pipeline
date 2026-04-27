# Batch Clickstream ETL Pipeline

<p align="center">
  <img src="Assets/batch clickstream etl pipeline.png" alt="Batch Clickstream ETL Pipeline" width="900" />
</p>

A production-style batch data pipeline that ingests clickstream events, lands them in a Bronze data lake, loads them into CrateDB, transforms them with dbt into Staging/Silver/Gold layers, syncs Gold metrics to Supabase, and refreshes Metabase dashboards. The pipeline is orchestrated with Dagster and instrumented with OpenObserve for observability.

## What This Project Does

- Pulls clickstream events from an external API using a watermark-based incremental fetch.
- Writes raw events to partitioned Parquet files in Bronze storage.
- Loads raw events into CrateDB (`raw.clickstream_events`).
- Runs dbt models for:
  - Staging cleanup and type normalization.
  - Silver deduplication with incremental logic.
  - Gold daily URL performance aggregations.
- Syncs Gold metrics to Supabase PostgreSQL.
- Triggers Metabase card refresh for near-real-time BI updates.
- Emits orchestration and stage-level telemetry/logs to OpenObserve.

## End-to-End Flow

1. External FastAPI clickstream API exposes raw event data.
2. Dagster schedule triggers an incremental pipeline run.
3. Ingestion fetches events after the last watermark.
4. Bronze writer stores raw events in partitioned Parquet.
5. Raw loader inserts events into CrateDB raw table.
6. dbt runs Staging, Silver, and Gold models with tests.
7. Gold metrics are materialized in CrateDB analytics schema.
8. Gold dataset is synced from CrateDB to Supabase.
9. Metabase cards are re-queried to reflect latest data.
10. Watermark is finalized for the next incremental cycle.

## Final Data Flow

```text
External FastAPI API
  ↓
Dagster (incremental ingestion)
  ↓
Bronze Layer (Parquet)
  ↓
dbt Tests (quality gate)
  ↓
Silver Layer (cleaned incremental data)
  ↓
Gold Layer (aggregations)
  ↓
CrateDB (warehouse)
  ↓
Supabase (serving layer)
  ↓
Metabase (dashboards)

Parallel observability:
Dagster logs + stage telemetry → OpenObserve
```

## Tech Stack

- Orchestration: Dagster
- Transformations: dbt
- Warehouse: CrateDB
- Serving Layer: Supabase PostgreSQL
- BI: Metabase
- Observability: OpenObserve (OTLP + event logs)
- Language: Python 3.11

## Repository Layout

```text
src/
  ingestion/         API fetch + normalization + watermark
  lakehouse/         Bronze parquet writer
  warehouse/         CrateDB load, dbt runner, Supabase sync
  bi/                Metabase API integration
  observability/     OpenObserve event logging + OTLP telemetry

dbt/
  models/staging/
  models/silver/
  models/gold/

orchestration/dagster_project/
  jobs/
  schedules/
  repository.py
  workspace.yaml

data/
  bronze/
  checkpoints/

Assets/
  batch clickstream etl pipeline.png
```

## Prerequisites

- Python 3.11+
- Docker Desktop
- Access to:
  - CrateDB cluster
  - Supabase project
  - Metabase instance/API key
  - Optional: OpenObserve instance

## Environment Variables

Create a local `.env` from `.env.example` and fill in values.

### Required core settings

- `CRATEDB_HOST`
- `CRATEDB_USERNAME`
- `CRATEDB_PASSWORD`
- `DBT_SCHEMA`
- `SUPABASE_DB_URL`
- `SUPABASE_GOLD_SCHEMA`
- `SUPABASE_GOLD_TABLE`
- `METABASE_ENABLED=true`
- `METABASE_URL`
- `METABASE_API_KEY` or (`METABASE_USERNAME` + `METABASE_PASSWORD`)
- `METABASE_CARD_IDS`

### OpenObserve settings

- `OPENOBSERVE_BASE_URL`
- `OPENOBSERVE_USERNAME`
- `OPENOBSERVE_PASSWORD`
- `OPENOBSERVE_INGEST_URL`
- `OPENOBSERVE_ORG`
- `OPENOBSERVE_STREAM`
- `ZO_ROOT_USER_EMAIL`
- `ZO_ROOT_USER_PASSWORD`

## Local Setup

1. Activate virtual environment.

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\venv\Scripts\Activate.ps1
```

2. Start Metabase.

```powershell
docker compose up -d
```

3. Start OpenObserve.

```powershell
docker compose -f docker-compose-openobserve.yml up -d
```

4. Start Dagster UI and daemon process.

```powershell
cd orchestration/dagster_project
dagster dev
```

Dagster loads `workspace.yaml`, which points to `repository.py` and registers:
- Job: `clickstream_pipeline_job`
- Schedule: `every_30_min_schedule` (currently configured with `*/5 * * * *`, i.e. every 5 minutes)

## Running dbt Manually (Optional)

```powershell
cd dbt
dbt debug --profiles-dir . --project-dir .
dbt run --select stg_clickstream_events --profiles-dir . --project-dir .
dbt test --select stg_clickstream_events --profiles-dir . --project-dir .
dbt run --select silver_clickstream_events --profiles-dir . --project-dir .
dbt test --select silver_clickstream_events --profiles-dir . --project-dir .
dbt run --select gold_url_daily_metrics --profiles-dir . --project-dir .
dbt test --select gold_url_daily_metrics --profiles-dir . --project-dir .
```

## Data Model Notes

### Staging (`stg_clickstream_events`)

- Trims/normalizes text fields.
- Casts timestamps and numeric IDs.
- Standardizes `country` to uppercase.

### Silver (`silver_clickstream_events`)

- Incremental model.
- Deduplicates by `event_id` with latest event preference.

### Gold (`gold_url_daily_metrics`)

- Daily URL and country-level aggregation.
- Metrics:
  - `click_count`
  - `unique_visitors` (distinct IP)
  - `unique_user_agents`
  - `last_event_ts`

## Observability

This project has automated observability at three layers:

1. Schedule-level event:
- `orchestration_schedule:triggered`

2. Pipeline-level events:
- `pipeline_start:start`
- `pipeline_complete:success`

3. Stage-level events for each Dagster op:
- start/success/error with duration and contextual metadata

Signals are emitted through:
- OpenTelemetry traces/metrics to OpenObserve OTLP endpoints
- Structured event logs to OpenObserve ingest endpoint

## Outputs and Artifacts

- Bronze files: `data/bronze/dt=YYYY-MM-DD/hour=HH/*.parquet`
- Watermark: `data/checkpoints/watermark.json`
- dbt compile/run artifacts: `dbt/target/`, `dbt/logs/`
- Dagster temp state: `orchestration/dagster_project/.tmp_dagster_home_*`

## Suggested .gitignore Coverage

Make sure runtime/generated paths are ignored:

```gitignore
venv/
.env
__pycache__/
*.pyc

# dbt artifacts
dbt/target/
dbt/logs/
dbt_packages/

# Dagster local runtime
orchestration/dagster_project/.tmp_dagster_home_*/
orchestration/dagster_project/.dagster/

# Local data outputs
data/bronze/
data/checkpoints/
orchestration/dagster_project/data/
```

## Troubleshooting

### Dagster load error for schedule keyword

If you see an error about `ScheduleDefinition.__init__()` and an unexpected keyword, use `execution_fn` (not `evaluation_fn`) for your installed Dagster version.

### OpenObserve auth fails

Ensure these are aligned:
- `OPENOBSERVE_USERNAME` and `OPENOBSERVE_PASSWORD`
- `ZO_ROOT_USER_EMAIL` and `ZO_ROOT_USER_PASSWORD`

### dbt connection failures

Verify:
- CrateDB credentials in `.env`
- `DBT_SCHEMA`
- SSL settings and reachable `CRATEDB_HOST`

## License

Add your preferred license here (MIT/Apache-2.0/etc.).
