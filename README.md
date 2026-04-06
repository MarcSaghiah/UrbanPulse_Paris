# Urban Pulse Paris

Worked on: January 2026

Stack: Python, Airflow, AWS S3, dbt, Athena, Kafka, Docker, Terraform, FreatExpectations, Streamlit

Urban Pulse Paris is an example of an end-to-end data engineering project built on open data from Velib Metropole. The platform ingests operational data, stores it in a Medallion architecture on AWS S3, orchestrates transformations with Airflow, models analytics with dbt, validates data quality, and exposes KPI analytics through Athena.

## Objectives

- Build a realistic data engineering workflow (batch and stream)
- Implement Bronze/Silver/Gold layers on S3
- Orchestrate production-like pipelines with Airflow
- Produce analytics-ready KPI datasets for BI/dashboarding
- Demonstrate reproducibility with Docker and Infrastructure as Code

## Architecture

### Data flow

1. Ingestion

- Batch: Velib API snapshot is collected and written locally
- Stream: Velib events are produced to Kafka and consumed into S3 Bronze stream prefix

2. Bronze

- Raw JSON snapshots are uploaded to S3

3. Silver

- Latest Bronze snapshot is normalized into partitioned Parquet

4. Gold

- KPI snapshot is built from Silver (station count, active stations, bike/dock counts)

5. Analytics

- Athena external table + view over Gold
- dbt models create staging/intermediate/mart layers

### Main tools

- AWS S3 (data lake)
- Athena (serverless SQL)
- Airflow (orchestration)
- Kafka + Kafka UI (streaming)
- Python (boto3, pandas, pyarrow)
- dbt (analytics modeling)
- Great Expectations (data quality)
- Terraform (IaC)
- Docker Compose (local reproducible stack)

## Project Structure

```text
airflow/dags/           Orchestration DAGs
ingestion/batch/        Batch ingestion + Bronze/Silver/Gold scripts
ingestion/stream/       Kafka producer and consumer
spark/                  Spark transformation scripts
dbt/                    dbt project and SQL models
great_expectations/     Data quality config, expectations, checkpoints
terraform/              AWS infrastructure definitions
.github/workflows/      CI workflow
```

## Why `data/` or `dashboards/` may look empty

- `data/raw/velib/` is local runtime output. Files appear after running `dag_velib_ingestion`.
- Most durable data is stored in S3 Bronze/Silver/Gold, not committed to Git.
- `dashboards/looker_studio_export.json` is a dashboard export placeholder. Visual dashboards are typically managed in external BI tools.

## Prerequisites

- Python 3.10+
- Docker Desktop (engine running)
- AWS CLI configured
- Terraform installed
- AWS account with permissions for S3, Athena, Glue, IAM

## Environment Variables

Start by copying `.env.example` to `.env`, then adapt values:

```bash
cp .env.example .env
```

If `cp` is unavailable on Windows PowerShell, use:

```powershell
Copy-Item .env.example .env
```

Required variables:

```dotenv
AWS_REGION=eu-west-3

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_ACCESS_KEY=
AWS_SECRET_KEY=

S3_BRONZE_BUCKET=urban-pulse-paris-bronze
S3_SILVER_BUCKET=urban-pulse-paris-silver
S3_GOLD_BUCKET=urban-pulse-paris-gold
ATHENA_RESULTS_BUCKET=urban-pulse-paris-athena-results

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=change_me
AIRFLOW_FERNET_KEY=

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_VELIB_TOPIC=velib-paris
```

## Quick Start

1. Install dependencies

```bash
pip install -r requirements.txt
```

2. Start platform

```bash
docker compose up -d
```

3. Open UIs

- Airflow: `http://localhost:8080`
- Kafka UI: `http://localhost:8081`

4. Run core DAGs in order

- dag_velib_ingestion
- dag_velib_bronze_upload
- dag_velib_silver_transform
- dag_velib_gold_kpis

## Reproducibility

If someone clones the repository and wants to validate the project quickly, this is the shortest path:

1. Create `.env` from the Environment Variables section with valid AWS credentials and bucket names.
2. Run `pip install -r requirements.txt`.
3. Run `docker compose up -d`.
4. Trigger in Airflow: `dag_velib_ingestion`, `dag_velib_bronze_upload`, `dag_velib_silver_transform`, `dag_velib_gold_kpis`.
5. Run `streamlit run dashboards/streamlit_app.py` and open `http://localhost:8501`.

## Expected Outputs

After a successful run:

1. Local raw snapshot files exist in `data/raw/velib/`.
2. Bronze objects exist in `s3://<bronze_bucket>/bronze/velib/status/`.
3. Silver parquet exists in `s3://<silver_bucket>/silver/velib/station_status/year=.../month=.../day=.../hour=.../`.
4. Gold parquet exists in `s3://<gold_bucket>/gold/velib/kpis_station_snapshot/year=.../month=.../day=.../hour=.../`.
5. Streamlit dashboard displays KPI cards and time series without errors.

## Dashboard

The runnable dashboard is located in `dashboards/streamlit_app.py`.

Run it with:

```bash
streamlit run dashboards/streamlit_app.py
```

The app reads Gold data directly from S3 and renders:

- Latest KPI cards
- Bikes available over time
- Active station ratio over time
- E-bike share over time
- Latest snapshot table

## Data Layer Conventions

### Bronze

```text
s3://<bronze_bucket>/bronze/velib/status/*.json
s3://<bronze_bucket>/bronze/velib/stream/*.json
```

### Silver

```text
s3://<silver_bucket>/silver/velib/station_status/year=YYYY/month=MM/day=DD/hour=HH/*.parquet
```

### Gold

```text
s3://<gold_bucket>/gold/velib/kpis_station_snapshot/year=YYYY/month=MM/day=DD/hour=HH/*.parquet
```

## Athena Setup

Run once:

```sql
CREATE DATABASE IF NOT EXISTS urban_pulse_paris;

CREATE EXTERNAL TABLE IF NOT EXISTS urban_pulse_paris.velib_kpis_station_snapshot (
    collected_at_utc string,
    station_count int,
    active_station_count int,
    total_bikes_available int,
    total_docks_available int,
    total_ebikes_available int,
    total_mechanical_bikes_available int
)
PARTITIONED BY (year int, month int, day int, hour int)
STORED AS PARQUET
LOCATION 's3://urban-pulse-paris-gold/gold/velib/kpis_station_snapshot/';

MSCK REPAIR TABLE urban_pulse_paris.velib_kpis_station_snapshot;
```

View used for dashboarding:

```sql
CREATE OR REPLACE VIEW urban_pulse_paris.vw_velib_kpis AS
SELECT
    CAST(from_iso8601_timestamp(collected_at_utc) AS timestamp) AS collected_at_ts,
    station_count,
    active_station_count,
    total_bikes_available,
    total_docks_available,
    total_ebikes_available,
    total_mechanical_bikes_available,
    CASE
        WHEN station_count = 0 THEN 0
        ELSE CAST(active_station_count AS DOUBLE) / station_count
    END AS active_station_ratio
FROM urban_pulse_paris.velib_kpis_station_snapshot;
```

## dbt

Commands:

```bash
dbt debug --project-dir dbt --profiles-dir dbt
dbt run --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt
```

Models included:

- stg_velib_kpis
- int_velib_kpis_enriched
- mart_velib_hourly_kpis

## Data Quality

Quality checks run in Airflow through dag_data_quality and include sanity checks such as:

- station_count > 0
- total_bikes_available >= 0
- total_docks_available >= 0

Great Expectations baseline configuration is available under great_expectations.

## Streaming Validation

Run producer and consumer locally:

```bash
python ingestion/stream/producer_velib_status.py
python ingestion/stream/consumer_velib_to_s3.py
```

Verify stream files:

```bash
aws s3 ls s3://urban-pulse-paris-bronze/bronze/velib/stream/ --recursive
```

## CI

CI workflow includes:

- Python syntax checks for ingestion and DAG scripts
- Terraform init/validate

## Cost Control Recommendations

- Keep Athena scans small by querying partitioned prefixes
- Set AWS Budgets alerts at low thresholds
- Configure S3 lifecycle policies for Bronze stream retention
- Use least-privilege IAM for production

## Summary

This project demonstrates a complete, production-inspired data engineering pipeline:

- Ingestion (batch + stream)
- Orchestration
- Lakehouse-style layering
- SQL modeling and testing
- Data quality checks
- Cloud analytics exposure
