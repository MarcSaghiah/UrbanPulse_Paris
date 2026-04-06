# Dashboards

This folder now contains a runnable dashboard application and SQL queries.

## What is available

- streamlit_app.py: local dashboard app fed by Gold parquet files in S3
- queries.sql: Athena SQL used for BI tools (Looker Studio/QuickSight)
- looker_studio_export.json: placeholder for future exported config

## Run the dashboard locally

1. Install dependencies:

```bash
pip install -r requirements.txt
```

1. Verify your .env contains at least:

```dotenv
AWS_REGION=eu-west-3
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_GOLD_BUCKET=urban-pulse-paris-gold
```

1. Ensure Gold data exists (run dag_velib_gold_kpis in Airflow).

1. Start dashboard:

```bash
streamlit run dashboards/streamlit_app.py
```

1. Open the URL shown by Streamlit (default [http://localhost:8501](http://localhost:8501)).

## What the dashboard shows

1. Latest KPI cards (stations, active stations, bikes, docks)
2. Bikes available over time
3. Active station ratio over time
4. E-bike share over time
5. Latest snapshots table
