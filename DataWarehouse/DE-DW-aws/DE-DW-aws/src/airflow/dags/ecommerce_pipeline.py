"""Pipeline de extremo a extremo en AWS: RDS MySQL + DocumentDB -> S3 -> Redshift -> dbt.

Orden: validación -> extracción a S3 (en paralelo por fuente) -> COPY a Redshift ->
verificación de conteos -> dbt (seed, intermediate, quarantine, snapshot, marts, test) ->
registro del resultado. Cada tarea reintenta dos veces; si agota los reintentos, el callback
marca el lote como failed en ops.batch_control con la tarea y el error.

Parámetros (Trigger DAG w/ config):
    batch_id   identificador del lote; vacío = fecha lógica de la corrida (YYYYMMDDTHHMMSSZ).
               Repetir un batch_id existente reprocesa sus archivos de S3 sin consultar las fuentes.
    mode       incremental (desde la última marca de agua exitosa) o full (todo el origen).
    fail_task  id de una tarea que debe fallar a propósito (p. ej. load_redshift_raw).
"""

from datetime import datetime, timedelta, timezone

from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from ecommerce_dw.ingestion.airflow_tasks import (
    extract_task,
    load_task,
    mark_status_task,
    on_task_failure,
    register_result_task,
    validate_task,
    verify_task,
)
from ecommerce_dw.transformations.dbt_runner import run_dbt

with DAG(
    dag_id="ecommerce_pipeline",
    description="RDS + DocumentDB -> S3 -> Redshift -> dbt, con control de lotes",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    # Manual para controlar costos; se puede cambiar por "0 6 * * *".
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param("", type="string", description="Vacío = fecha lógica de la corrida"),
        "mode": Param("incremental", type="string", enum=["incremental", "full"]),
        "fail_task": Param("", type="string", description="Tarea que fallará a propósito (vacío = ninguna)"),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_task_failure,
    },
    tags=["aws", "redshift", "dbt"],
) as dag:
    validate = PythonOperator(task_id="validate_connections_and_params", python_callable=validate_task)

    with TaskGroup(group_id="extract_to_s3", tooltip="Extracción por lote a s3://<bucket>/raw/"):
        extract_mysql = PythonOperator(task_id="rds_mysql", python_callable=extract_task, op_args=["mysql"])
        extract_mongo = PythonOperator(task_id="documentdb", python_callable=extract_task, op_args=["documentdb"])

    load = PythonOperator(task_id="load_redshift_raw", python_callable=load_task)
    verify = PythonOperator(task_id="verify_raw_counts", python_callable=verify_task)

    with TaskGroup(group_id="dbt", tooltip="Transformaciones ejecutadas en Redshift"):
        deps = PythonOperator(task_id="dbt_deps", python_callable=run_dbt, op_args=["deps"])
        seed = PythonOperator(task_id="dbt_seed", python_callable=run_dbt, op_args=["seed"])
        intermediate = PythonOperator(task_id="dbt_run_intermediate", python_callable=run_dbt,
                                      op_args=["run", "path:models/intermediate"])
        quarantine = PythonOperator(task_id="dbt_run_quarantine", python_callable=run_dbt,
                                    op_args=["run", "path:models/quarantine"])
        snapshots = PythonOperator(task_id="dbt_snapshot", python_callable=run_dbt, op_args=["snapshot"])
        marts = PythonOperator(task_id="dbt_run_marts", python_callable=run_dbt,
                               op_args=["run", "path:models/marts"])
        transformed = PythonOperator(task_id="mark_transformed", python_callable=mark_status_task,
                                     op_args=["transformed"])
        tests = PythonOperator(task_id="dbt_test", python_callable=run_dbt, op_args=["test"])

        deps >> seed >> intermediate >> quarantine >> snapshots >> marts >> transformed >> tests

    register = PythonOperator(task_id="register_result", python_callable=register_result_task)

    validate >> [extract_mysql, extract_mongo] >> load >> verify >> deps
    tests >> register
