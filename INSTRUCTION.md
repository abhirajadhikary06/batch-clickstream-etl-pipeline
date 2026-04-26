# URL Shortener Analytics Pipeline

## Batch Medallion Data Engineering System (Incremental + Lakehouse Architecture)

---

# 1. Overview

This project implements a batch-oriented data engineering pipeline that processes clickstream events from an external URL shortener system exposed via a FastAPI API. The pipeline performs incremental ingestion, transforms raw data using a medallion architecture (Bronze → Silver → Gold), and serves analytics through a warehouse, query engine, and BI dashboard layer.

The system is designed to simulate real-world production data engineering workflows including incremental ingestion, orchestration, data quality enforcement, observability, and scalable analytics querying.

---

# 2. High-Level Architecture

```text id="arch_001"

                ┌──────────────────────────────────────────┐
                │ External Source System                  │
                │ FastAPI Clickstream API                │
                │                                          │
                │ - provides event data                  │
                │ - acts as upstream data source        │
                └───────────────┬──────────────────────────┘
                                │
                                ▼
                ┌──────────────────────────────────────────┐
                │ Dagster Ingestion Layer                │
                │                                          │
                │ Responsibilities:                      │
                │ - pulls incremental data (watermark)   │
                │ - ensures no full reprocessing         │
                │ - schedules every 30 minutes           │
                └───────────────┬──────────────────────────┘
                                │
                                ▼
        ┌──────────────────────────────────────────────────────┐
        │ Bronze Layer (Raw Data Lake)                        │
        │                                                      │
        │ Storage: Parquet                                     │
        │ Path: data/bronze/dt=YYYY-MM-DD/                   │
        │                                                      │
        │ Tasks:                                              │
        │ - store raw incremental events                      │
        │ - append-only ingestion                            │
        │ - partition by time                               │
        └───────────────┬──────────────────────────────────────┘
                        │
                        ▼
        ┌──────────────────────────────────────────────────────┐
        │ dbt Data Quality Layer                             │
        │                                                      │
        │ Tool: dbt tests                                     │
        │                                                      │
        │ Checkpoints:                                       │
        │ - not_null constraints                            │
        │ - unique event_id                                │
        │ - timestamp validation                            │
        │ - referential integrity checks                   │
        │ - duplicate detection                            │
        └───────────────┬──────────────────────────────────────┘
                        │
                        ▼
        ┌──────────────────────────────────────────────────────┐
        │ Silver Layer (Cleaned Data)                         │
        │                                                      │
        │ Tool: dbt incremental models                        │
        │ Storage: Parquet                                     │
        │                                                      │
        │ Tasks:                                              │
        │ - remove duplicates                                │
        │ - normalize schema                                 │
        │ - standardize fields                               │
        │ - filter corrupted records                         │
        │ - incremental processing only                      │
        └───────────────┬──────────────────────────────────────┘
                        │
                        ▼
        ┌──────────────────────────────────────────────────────┐
        │ Gold Layer (Business Aggregations)                 │
        │                                                      │
        │ Tool: dbt models (incremental)                     │
        │ Storage: Parquet                                     │
        │                                                      │
        │ Tasks:                                              │
        │ - clicks per URL                                   │
        │ - unique users per URL                             │
        │ - geo analytics                                    │
        │ - time-series trends                              │
        │ - top URLs                                         │
        └───────────────┬──────────────────────────────────────┘
                        │
                        ▼
        ┌──────────────────────────────────────────────────────┐
        │ CrateDB (Time-Series Warehouse)                    │
        │                                                      │
        │ Responsibilities:                                  │
        │ - stores Gold layer datasets                       │
        │ - optimized for analytical queries                │
        │ - serves as primary data warehouse                │
        └───────────────┬──────────────────────────────────────┘
                        │
                        ▼
        ┌──────────────────────────────────────────────────────┐
        │ Metabase (BI & Dashboard Layer)                    │
        │                                                      │
        │ Responsibilities:                                  │
        │ - dashboards & KPI reporting                       │
        │ - ad-hoc analytics                                │
        │ - auto-refresh via query re-execution             │
        │ - always reflects latest warehouse state          │
        └──────────────────────────────────────────────────────┘


──────────────────────────────────────────────────────────────

Observability Layer

        ┌──────────────────────────────────────────────────────┐
        │ OpenObserve                                       │
        │                                                      │
        │ Responsibilities:                                  │
        │ - Dagster pipeline logs                           │
        │ - ingestion metrics                               │
        │ - transformation metrics                          │
        │ - failure tracking                                │
        │ - data freshness monitoring                       │
        └──────────────────────────────────────────────────────┘
```

---

# 3. Core Design Principles

* External system as **data source (API-driven ingestion)**
* Strict **incremental ingestion (no full reloads)**
* Batch processing every 30 minutes
* Medallion architecture (Bronze → Silver → Gold)
* Parquet-based data lake storage
* dbt-based transformation + validation
* Query abstraction via Trino
* BI decoupled from storage layer
* Observability-first pipeline design

---

# 4. Incremental Ingestion Strategy

## Key Concept: Watermark-based ingestion

Dagster maintains a **last successful ingestion timestamp**

Each run:

```text id="inc_01"
SELECT * FROM API
WHERE event_timestamp > last_watermark
```

### Guarantees:

* no duplicate ingestion
* no full dataset reload
* scalable as data grows

---

# 5. Component Responsibilities

---

## 5.1 External FastAPI Source

* provides clickstream events via API
* acts as upstream system
* not part of pipeline computation

---

## 5.2 Dagster (Orchestration Layer)

Dagster

* schedules ingestion every 30 minutes
* manages incremental pull logic
* orchestrates Bronze → Gold pipeline
* tracks lineage and execution state
* ensures failure recovery

---

## 5.3 Bronze Layer

* stores raw incremental data
* Parquet-based storage
* append-only model
* time partitioned structure

---

## 5.4 dbt Data Quality Layer

dbt

* schema validation
* uniqueness constraints
* timestamp validation
* duplicate detection
* acts as pipeline quality gate

---

## 5.5 Silver Layer

* cleans raw data
* removes duplicates
* standardizes schema
* incremental transformations only

---

## 5.6 Gold Layer

* business aggregations
* KPI generation
* time-series analytics
* URL performance metrics

---

## 5.7 CrateDB (Warehouse)

CrateDB

* stores final curated datasets
* optimized for analytical queries
* serves as primary warehouse

---

## 5.8 Trino (Query Engine)

Trino

* executes SQL queries
* forwards to CrateDB
* query abstraction layer for BI tools

---

## 5.9 Metabase (BI Layer)

Metabase

* dashboard creation
* KPI visualization
* query execution via Trino
* auto-refresh reflects latest warehouse state

---

## 5.10 OpenObserve (Observability Layer)

* pipeline logs tracking
* performance monitoring
* failure detection
* data freshness alerts

---

# 6. Final Data Flow

```text id="flow_001"
External FastAPI API
        ↓
Dagster (incremental ingestion every 30 min)
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
Trino (query engine)
        ↓
Metabase (dashboards)

Parallel:
Dagster logs → OpenObserve
```

---

# 7. System Characteristics

### Strengths

* real-world ingestion pattern (external API source)
* incremental pipeline (production-grade design)
* clean medallion architecture
* strong separation of concerns
* scalable transformation design
* proper query abstraction layer
* observability integration

### Trade-offs

* batch system (not real-time streaming)
* additional complexity due to Trino layer
* simplified local Parquet-based lake

---

# 8. Final Summary

This project simulates a production-grade batch analytics system that processes clickstream data from an external API using incremental ingestion. It demonstrates:

* real-world ingestion design (watermark-based)
* modern medallion architecture
* dbt-based data quality enforcement
* orchestration via Dagster
* query abstraction via Trino
* BI layer separation via Metabase
* observability integration

