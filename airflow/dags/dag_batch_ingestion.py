from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
	dag_id="dag_batch_ingestion",
	start_date=datetime(2024, 1, 1),
	schedule="*/15 * * * *",
	catchup=False,
	tags=["batch", "orchestration", "velib"],
) as dag:
	ingest_local = TriggerDagRunOperator(
		task_id="trigger_velib_ingestion",
		trigger_dag_id="dag_velib_ingestion",
		wait_for_completion=True,
	)

	upload_bronze = TriggerDagRunOperator(
		task_id="trigger_bronze_upload",
		trigger_dag_id="dag_velib_bronze_upload",
		wait_for_completion=True,
	)

	ingest_local >> upload_bronze

