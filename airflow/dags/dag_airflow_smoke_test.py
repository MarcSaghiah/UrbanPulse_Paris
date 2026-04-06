from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def log_start() -> None:
    print("Urban Pulse Paris smoke test: start")


def log_execution_context(**context) -> None:
    print(f"Run id: {context.get('run_id')}")
    print(f"Logical date: {context.get('logical_date')}")


default_args = {
    "owner": "marcsag",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_airflow_smoke_test",
    default_args=default_args,
    description="Quick health check DAG to validate Airflow scheduling",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["bootstrap", "healthcheck"],
) as dag:
    start = PythonOperator(task_id="log_start", python_callable=log_start)

    show_context = PythonOperator(
        task_id="log_execution_context",
        python_callable=log_execution_context,
    )

    start >> show_context
