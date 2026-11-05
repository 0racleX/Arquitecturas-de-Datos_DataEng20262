"""Extracción por lote a S3 y carga a Redshift con COPY.

Ruta de cada lote:  s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/
    <entidad>.csv.gz   datos + columnas _batch_id, _extracted_at, _source
    _manifest.json     filas, fecha de extracción, marcas de agua y columnas

El manifiesto se escribe al final y con escritura condicional (If-None-Match): si ya existe,
el lote está extraído y se reutiliza sin consultar las fuentes; nunca se sobrescribe.
"""

import csv
import gzip
import json
import logging
import os
import tempfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from . import redshift
from .raw_sources import ENTITIES, csv_value, source_rows

LOGGER = logging.getLogger(__name__)


def bucket():
    return os.environ["DE_DW_BUCKET"]


def s3_client():
    return boto3.client("s3", region_name=os.getenv("AWS_REGION"))


def batch_prefix(entity, batch_id):
    return f"raw/{entity.source}/{entity.name}/batch_id={batch_id}/"


def manifest_key(entity, batch_id):
    return batch_prefix(entity, batch_id) + "_manifest.json"


def data_key(entity, batch_id):
    return batch_prefix(entity, batch_id) + f"{entity.name}.csv.gz"


def read_manifest(entity, batch_id):
    try:
        body = s3_client().get_object(Bucket=bucket(), Key=manifest_key(entity, batch_id))["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body)


def _put_if_absent(key, body, content_type):
    """Escritura condicional: falla con PreconditionFailed si el objeto ya existe."""
    try:
        s3_client().put_object(Bucket=bucket(), Key=key, Body=body, ContentType=content_type, IfNoneMatch="*")
    except ClientError as error:
        if error.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise FileExistsError(f"s3://{bucket()}/{key} already exists; refusing to overwrite it") from error
        raise


def _typed_row(entity, row):
    values = []
    for (column, kind), value in zip(entity.columns.items(), row):
        if kind == "BOOLEAN" and value is not None:
            value = bool(value)
        values.append(csv_value(value))
    return values


def extract_entity(entity, batch_id, watermark_from):
    """Extrae una entidad a S3. Si el manifiesto del lote existe, lo devuelve sin leer la fuente."""
    existing = read_manifest(entity, batch_id)
    if existing is not None:
        LOGGER.info("[batch=%s] %s.%s already extracted (%s rows); reusing S3 files",
                    batch_id, entity.source, entity.name, existing["rows"])
        return {**existing, "reused": True}

    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)
    watermark_index = list(entity.columns).index(entity.watermark)
    watermark_to = watermark_from
    rows = 0
    with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as handle:
        path = handle.name
    try:
        with gzip.open(path, "wt", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(list(entity.all_columns))
            for row in source_rows(entity, watermark_from):
                writer.writerow(_typed_row(entity, row) + [batch_id, extracted_at.isoformat(sep=" "), entity.source_label])
                value = row[watermark_index]
                if value is not None:
                    value = entity.parse_watermark(csv_value(value)) if entity.watermark_kind == "timestamp" else int(value)
                    watermark_to = value if watermark_to is None else max(watermark_to, value)
                rows += 1
        # Los datos sin manifiesto pertenecen a un intento incompleto de este mismo lote: se reemplazan.
        with open(path, "rb") as data:
            s3_client().put_object(Bucket=bucket(), Key=data_key(entity, batch_id), Body=data,
                                   ContentType="application/gzip")
    finally:
        os.unlink(path)

    def as_text(value):
        if value is None:
            return None
        return value.isoformat(sep=" ") if isinstance(value, datetime) else str(value)

    manifest = {
        "batch_id": batch_id,
        "source": entity.source,
        "entity": entity.name,
        "raw_table": entity.raw_table,
        "rows": rows,
        "extracted_at": extracted_at.isoformat(sep=" "),
        "mode": "full" if watermark_from is None else "incremental",
        "watermark_column": entity.watermark,
        "watermark_from": as_text(watermark_from),
        "watermark_to": as_text(watermark_to),
        "s3_uri": f"s3://{bucket()}/{data_key(entity, batch_id)}",
        "format": "csv+gzip",
        "columns": list(entity.all_columns),
    }
    _put_if_absent(manifest_key(entity, batch_id), json.dumps(manifest, indent=2).encode(), "application/json")
    LOGGER.info("[batch=%s] extracted %s rows from %s.%s (watermark %s -> %s) to %s",
                batch_id, rows, entity.source, entity.name, manifest["watermark_from"],
                manifest["watermark_to"], manifest["s3_uri"])
    return {**manifest, "reused": False}


def extract_source(source, batch_id, mode):
    results = {}
    for entity in [entity for entity in ENTITIES if entity.source == source]:
        watermark_from = None if mode == "full" else redshift.last_watermark(entity, batch_id)
        results[entity.name] = extract_entity(entity, batch_id, watermark_from)
    return results


def _sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def load_entity(entity, batch_id):
    """Carga idempotente: borra las filas del lote en raw y las vuelve a copiar en una transacción."""
    manifest = read_manifest(entity, batch_id)
    if manifest is None:
        raise FileNotFoundError(f"[batch={batch_id}] manifest missing for {entity.source}.{entity.name}")
    columns = ", ".join(manifest["columns"])
    copy_sql = (
        f"COPY {entity.raw_table} ({columns}) FROM {_sql_literal(manifest['s3_uri'])} "
        f"IAM_ROLE {_sql_literal(os.environ['REDSHIFT_IAM_ROLE_ARN'])} "
        f"FORMAT AS CSV IGNOREHEADER 1 GZIP EMPTYASNULL "
        f"TIMEFORMAT 'auto' DATEFORMAT 'auto' REGION {_sql_literal(os.environ['AWS_REGION'])}"
    )
    redshift.execute([
        (f"DELETE FROM {entity.raw_table} WHERE _batch_id = %s", (batch_id,)),
        (copy_sql, None),
        ("DELETE FROM ops.extraction_log WHERE batch_id = %s AND source = %s AND entity = %s",
         (batch_id, entity.source, entity.name)),
        (f"""INSERT INTO ops.extraction_log
             (batch_id, source, entity, rows_extracted, rows_loaded, watermark_column, watermark_from,
              watermark_to, s3_uri, extracted_at, loaded_at)
             SELECT %s, %s, %s, %s, COUNT(*), %s, %s, %s, %s, CAST(%s AS TIMESTAMP), GETDATE()
             FROM {entity.raw_table} WHERE _batch_id = %s""",
         (batch_id, entity.source, entity.name, manifest["rows"], manifest["watermark_column"],
          manifest["watermark_from"] or "", manifest["watermark_to"] or "", manifest["s3_uri"],
          manifest["extracted_at"], batch_id)),
    ])
    LOGGER.info("[batch=%s] COPY %s <- %s", batch_id, entity.raw_table, manifest["s3_uri"])
    return manifest


def verify_batch_counts(batch_id):
    """Compara las filas del manifiesto con las cargadas en raw para el lote."""
    _, rows = redshift.query(
        "SELECT source, entity, rows_extracted, rows_loaded FROM ops.extraction_log WHERE batch_id = %s",
        (batch_id,),
    )
    found = {(source, entity): (int(extracted), int(loaded)) for source, entity, extracted, loaded in rows}
    expected = {(entity.source, entity.name) for entity in ENTITIES}
    missing = expected - set(found)
    mismatches = {key: value for key, value in found.items() if value[0] != value[1]}
    if missing or mismatches:
        raise ValueError(f"[batch={batch_id}] raw verification failed; missing={missing} mismatches={mismatches}")
    LOGGER.info("[batch=%s] S3 manifest and raw row counts match: %s", batch_id, found)
    return {f"{source}.{entity}": counts[1] for (source, entity), counts in found.items()}
