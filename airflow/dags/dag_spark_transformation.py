from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
	dag_id="dag_spark_transformation",
	start_date=datetime(2024, 1, 1),
	schedule=None,
	catchup=False,
	tags=["spark", "silver", "orchestration"],
) as dag:
	trigger_silver = TriggerDagRunOperator(
		task_id="trigger_velib_silver_transform",
		trigger_dag_id="dag_velib_silver_transform",
		wait_for_completion=True,
	)

	trigger_silver

