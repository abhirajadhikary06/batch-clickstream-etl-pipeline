FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	PIP_DISABLE_PIP_VERSION_CHECK=1 \
	PIP_NO_CACHE_DIR=1 \
	POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

# Minimal system packages for wheels, TLS certs, and common data tooling.
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		build-essential \
		curl \
		git \
		libpq-dev \
		ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Install the runtime dependencies used across ingestion, orchestration, dbt, and sync jobs.
RUN python -m pip install --upgrade pip setuptools wheel \
	&& python -m pip install \
		dagster \
		dagster-webserver \
		dbt-core \
		dbt-cratedb \
		pandas \
		pyarrow \
		requests \
		python-dotenv \
		crate \
		psycopg2-binary \
		opentelemetry-api \
		opentelemetry-sdk \
		opentelemetry-exporter-otlp-proto-http

COPY . /app

ENV PYTHONPATH=/app \
	DAGSTER_HOME=/app/orchestration/dagster_project/.dagster \
	DBT_PROFILES_DIR=/app/dbt \
	DBT_PROJECT_DIR=/app/dbt

RUN mkdir -p "$DAGSTER_HOME"

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-f", "orchestration/dagster_project/repository.py", "-w", "orchestration/dagster_project/workspace.yaml"]
