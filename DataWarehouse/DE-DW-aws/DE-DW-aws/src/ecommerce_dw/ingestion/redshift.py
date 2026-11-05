"""Conexión a Redshift, DDL de raw y tablas de control de lotes (esquema ops)."""

import os
from contextlib import closing

import redshift_connector

from .raw_sources import ENTITIES

# RAW es palabra reservada en Redshift; por eso la capa raw vive en el esquema raw_data.
RAW_SCHEMA = "raw_data"

OPS_DDL = [
    "CREATE SCHEMA IF NOT EXISTS ops",
    f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}",
    """CREATE TABLE IF NOT EXISTS ops.batch_control (
        batch_id VARCHAR(64) NOT NULL,
        dag_run_id VARCHAR(256),
        mode VARCHAR(16),
        status VARCHAR(32),
        started_at TIMESTAMP,
        updated_at TIMESTAMP,
        finished_at TIMESTAMP,
        failed_task VARCHAR(256),
        message VARCHAR(4096)
    )""",
    """CREATE TABLE IF NOT EXISTS ops.extraction_log (
        batch_id VARCHAR(64) NOT NULL,
        source VARCHAR(32),
        entity VARCHAR(64),
        rows_extracted BIGINT,
        rows_loaded BIGINT,
        watermark_column VARCHAR(64),
        watermark_from VARCHAR(64),
        watermark_to VARCHAR(64),
        s3_uri VARCHAR(1024),
        extracted_at TIMESTAMP,
        loaded_at TIMESTAMP
    )""",
]


def connect():
    return redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        database=os.environ["REDSHIFT_DATABASE"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
        ssl=True,
        timeout=60,
    )


def query(sql, params=None):
    """Ejecuta una consulta de lectura y devuelve (columnas, filas)."""
    with closing(connect()) as connection:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        columns = [description[0] for description in cursor.description or []]
        rows = cursor.fetchall() if cursor.description else []
        connection.commit()
        return columns, list(rows)


def execute(statements):
    """Ejecuta una lista de (sql, params) en una sola transacción."""
    with closing(connect()) as connection:
        cursor = connection.cursor()
        try:
            for sql, params in statements:
                cursor.execute(sql, params)
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def raw_table_ddl(entity):
    columns = ",\n    ".join(f"{name} {kind}" for name, kind in entity.all_columns.items())
    return f"CREATE TABLE IF NOT EXISTS {entity.raw_table} (\n    {columns},\n    _loaded_at TIMESTAMP DEFAULT GETDATE()\n)"


def ensure_schema():
    """Crea ops y raw_data con sus tablas si aún no existen. Es idempotente."""
    execute([(sql, None) for sql in OPS_DDL] + [(raw_table_ddl(entity), None) for entity in ENTITIES])


def set_batch_status(batch_id, status, dag_run_id=None, mode=None, failed_task=None, message=None, finished=False):
    """Inserta o actualiza el estado del lote. started_at se conserva en las reejecuciones."""
    with closing(connect()) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM ops.batch_control WHERE batch_id = %s", (batch_id,))
            exists = cursor.fetchone()[0] > 0
            finished_sql = "GETDATE()" if finished else "NULL"
            if exists:
                assignments = ["status = %s", "failed_task = %s", "message = %s", "updated_at = GETDATE()",
                               f"finished_at = {finished_sql}"]
                params = [status, failed_task or "", message or ""]
                for column, value in (("dag_run_id", dag_run_id), ("mode", mode)):
                    if value is not None:
                        assignments.append(f"{column} = %s")
                        params.append(value)
                cursor.execute(
                    f"UPDATE ops.batch_control SET {', '.join(assignments)} WHERE batch_id = %s",
                    (*params, batch_id),
                )
            else:
                cursor.execute(
                    f"""INSERT INTO ops.batch_control
                        (batch_id, dag_run_id, mode, status, started_at, updated_at, finished_at, failed_task, message)
                        VALUES (%s, %s, %s, %s, GETDATE(), GETDATE(), {finished_sql}, %s, %s)""",
                    (batch_id, dag_run_id or "", mode or "", status, failed_task or "", message or ""),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def last_watermark(entity, exclude_batch_id):
    """Marca de agua: el máximo watermark_to de los lotes en estado success.

    Un lote fallido no avanza la marca, así que el siguiente lote vuelve a leer ese rango.
    """
    _, rows = query(
        """SELECT log.watermark_to
           FROM ops.extraction_log AS log
           INNER JOIN ops.batch_control AS control ON control.batch_id = log.batch_id
           WHERE control.status = 'success'
             AND log.source = %s AND log.entity = %s AND log.batch_id <> %s
             AND log.watermark_to IS NOT NULL""",
        (entity.source, entity.name, exclude_batch_id),
    )
    values = [entity.parse_watermark(row[0]) for row in rows]
    values = [value for value in values if value is not None]
    return max(values) if values else None
