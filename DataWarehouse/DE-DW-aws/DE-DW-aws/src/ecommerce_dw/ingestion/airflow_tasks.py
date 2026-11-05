"""Funciones que ejecutan las tareas del DAG ecommerce_pipeline.

Cada tarea recalcula el batch_id a partir del contexto (parámetro batch_id o fecha lógica de la
corrida), así un reintento o una reejecución trabajan siempre sobre el mismo lote.
"""

import json
import logging
import os
import re

import boto3
from pymongo.errors import PyMongoError

from . import redshift
from .raw_ingestion import bucket, extract_source, load_entity, verify_batch_counts
from .raw_sources import ENTITIES, connect_mongo, connect_mysql

LOGGER = logging.getLogger(__name__)
BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
REQUIRED_ENV = [
    "DE_DW_BUCKET", "AWS_REGION", "MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MONGODB_URI",
    "REDSHIFT_HOST", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD", "REDSHIFT_IAM_ROLE_ARN",
]


def batch_context(context):
    """Devuelve (batch_id, mode) validados para la corrida."""
    params = context["params"]
    batch_id = (params.get("batch_id") or "").strip() or context["logical_date"].strftime("%Y%m%dT%H%M%SZ")
    if not BATCH_ID_PATTERN.match(batch_id):
        raise ValueError(f"Invalid batch_id {batch_id!r}: use letters, numbers, '_' or '-' (max 64)")
    mode = params.get("mode") or "incremental"
    if mode not in ("incremental", "full"):
        raise ValueError(f"Invalid mode {mode!r}: expected 'incremental' or 'full'")
    return batch_id, mode


def maybe_fail(context):
    """Fallo controlado: si el parámetro fail_task coincide con la tarea, la tarea falla."""
    target = (context["params"].get("fail_task") or "").strip()
    task_id = context["task"].task_id
    if target and (task_id == target or task_id.endswith("." + target)):
        raise RuntimeError(f"Controlled failure requested for task {task_id} (fail_task={target})")


def validate_task(**context):
    """1. Valida parámetros, variables y conexiones; crea ops/raw_data y marca el lote en running."""
    batch_id, mode = batch_context(context)
    missing = [name for name in REQUIRED_ENV if not os.getenv(name)]
    if missing:
        raise EnvironmentError(f"Missing environment variables: {missing}")

    connection = connect_mysql()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
    finally:
        connection.close()
    LOGGER.info("[batch=%s] RDS MySQL reachable at %s", batch_id, os.environ["MYSQL_HOST"])

    client = connect_mongo()
    try:
        client.admin.command("ping")
    except PyMongoError as error:
        raise ConnectionError(f"DocumentDB ping failed: {error}") from error
    finally:
        client.close()
    LOGGER.info("[batch=%s] DocumentDB reachable", batch_id)

    boto3.client("s3", region_name=os.environ["AWS_REGION"]).head_bucket(Bucket=bucket())
    LOGGER.info("[batch=%s] S3 bucket %s reachable", batch_id, bucket())

    redshift.ensure_schema()
    redshift.set_batch_status(batch_id, "running", dag_run_id=context["run_id"], mode=mode)
    LOGGER.info("[batch=%s] Redshift reachable; batch marked running (mode=%s)", batch_id, mode)
    maybe_fail(context)
    return {"batch_id": batch_id, "mode": mode}


def extract_task(source, **context):
    """2-3. Extrae una fuente y escribe sus archivos y manifiestos en S3."""
    batch_id, mode = batch_context(context)
    maybe_fail(context)
    results = extract_source(source, batch_id, mode)
    return {name: {"rows": result["rows"], "s3_uri": result["s3_uri"], "reused": result["reused"]}
            for name, result in results.items()}


def load_task(**context):
    """4. Carga a Redshift (raw_data) con COPY cada entidad del lote, en serie."""
    batch_id, _ = batch_context(context)
    for entity in ENTITIES:
        load_entity(entity, batch_id)
    maybe_fail(context)
    redshift.set_batch_status(batch_id, "raw_loaded")


def verify_task(**context):
    batch_id, _ = batch_context(context)
    maybe_fail(context)
    return verify_batch_counts(batch_id)


def mark_status_task(status, **context):
    batch_id, _ = batch_context(context)
    redshift.set_batch_status(batch_id, status)


def register_result_task(**context):
    """7. Registra el resultado: estado success en ops.batch_control y resumen JSON en S3."""
    batch_id, mode = batch_context(context)
    maybe_fail(context)
    _, raw_counts = redshift.query(
        "SELECT source, entity, rows_extracted, rows_loaded, watermark_from, watermark_to "
        "FROM ops.extraction_log WHERE batch_id = %s ORDER BY source, entity",
        (batch_id,),
    )
    facts = {}
    for table in ("fct_sales", "fct_reviews", "fct_navigation_events"):
        _, rows = redshift.query(f"SELECT COUNT(*) FROM marts.{table}")
        facts[table] = int(rows[0][0])
    redshift.set_batch_status(batch_id, "success", finished=True)

    summary = {
        "batch_id": batch_id,
        "mode": mode,
        "dag_run_id": context["run_id"],
        "status": "success",
        "entities": [
            {"source": s, "entity": e, "rows_extracted": int(x), "rows_loaded": int(l),
             "watermark_from": wf, "watermark_to": wt}
            for s, e, x, l, wf, wt in raw_counts
        ],
        "fact_rows": facts,
    }
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])
    key = f"control/batches/batch_id={batch_id}/result_{safe_run_id}.json"
    boto3.client("s3", region_name=os.environ["AWS_REGION"]).put_object(
        Bucket=bucket(), Key=key, Body=json.dumps(summary, indent=2).encode(), ContentType="application/json")
    LOGGER.info("[batch=%s] SUCCESS. Summary written to s3://%s/%s: %s", batch_id, bucket(), key, summary)
    return summary


def on_task_failure(context):
    """Callback tras agotar reintentos: deja el lote en failed con la tarea y el error."""
    try:
        batch_id, _ = batch_context(context)
    except Exception:  # noqa: BLE001 - el batch_id inválido ya quedó en el log de la tarea
        return
    task_id = context["task_instance"].task_id
    message = str(context.get("exception"))[:4000]
    LOGGER.error("[batch=%s] task %s FAILED: %s", batch_id, task_id, message)
    try:
        redshift.set_batch_status(batch_id, "failed", failed_task=task_id, message=message, finished=True)
    except Exception:  # noqa: BLE001 - Redshift caído: el fallo sigue visible en Airflow
        LOGGER.exception("[batch=%s] could not record failure in ops.batch_control", batch_id)
