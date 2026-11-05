"""Ejecuta comandos de dbt desde tareas de Airflow. El SQL lo ejecuta Redshift."""

import logging
import os
import re
import subprocess
from pathlib import Path

import boto3

from ecommerce_dw.ingestion.airflow_tasks import batch_context, maybe_fail

DBT_EXECUTABLE = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = Path("/opt/airflow/src/dbt")
LOGGER = logging.getLogger(__name__)


def run_dbt(command, selector=None, **context):
    """Ejecuta dbt; si falla, la tarea falla y Airflow aplica los reintentos configurados."""
    batch_id, _ = batch_context(context)
    maybe_fail(context)
    arguments = [
        DBT_EXECUTABLE,
        command,
        "--project-dir", str(DBT_PROJECT),
        "--profiles-dir", str(DBT_PROJECT),
    ]
    if selector is not None:
        arguments.extend(["--select", selector])
    if command != "deps":
        arguments.extend(["--threads", "4"])

    LOGGER.info("[batch=%s] Running dbt command: %s", batch_id, " ".join(arguments))
    subprocess.run(arguments, check=True, env=os.environ.copy())

    # Evidencia: conserva run_results.json de cada comando en S3 junto al lote.
    results = DBT_PROJECT / "target" / "run_results.json"
    if command != "deps" and results.exists():
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])
        key = f"control/batches/batch_id={batch_id}/dbt/{safe_run_id}/{command}_run_results.json"
        boto3.client("s3", region_name=os.environ["AWS_REGION"]).upload_file(
            str(results), os.environ["DE_DW_BUCKET"], key)
        LOGGER.info("[batch=%s] dbt results saved to s3://%s/%s", batch_id, os.environ["DE_DW_BUCKET"], key)
