from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def dbt_run_instructions() -> None:
	print("dbt execution is configured for local/CI runtime from the dbt folder.")
	print("Run: dbt build --profiles-dir dbt --project-dir dbt")


with DAG(
	dag_id="dag_dbt_run",
	start_date=datetime(2024, 1, 1),
	schedule=None,
	catchup=False,
	tags=["dbt", "transform"],
) as dag:
	run_dbt = PythonOperator(task_id="dbt_run_instructions", python_callable=dbt_run_instructions)

	run_dbt

