"""Genera terraform/glue_tables.tf a partir de src/news_pipeline/schemas.py.

  python scripts/gen_glue_tables_tf.py

Así los esquemas Parquet y las tablas del Glue Data Catalog no se desalinean.
"""
import os
import sys

import pyarrow as pa

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from news_pipeline import schemas as S  # noqa: E402


def hive(t: pa.DataType) -> str:
    if pa.types.is_string(t):
        return "string"
    if pa.types.is_int32(t):
        return "int"
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_float64(t):
        return "double"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_list(t):
        return f"array<{hive(t.value_type)}>"
    if pa.types.is_struct(t):
        return "struct<" + ",".join(f"{f.name}:{hive(f.type)}" for f in t) + ">"
    raise TypeError(t)


PARQUET = '''    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }'''

JSON = '''    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters            = { "ignore.malformed.json" = "true" }
    }'''


def cols(pairs) -> str:
    return "\n".join(f'    columns {{\n      name = "{n}"\n      type = "{t}"\n    }}' for n, t in pairs)


def table(name, location, comment, columns, fmt="parquet", partition=None, projection=None):
    params = [f'    classification = "{fmt}"', '    EXTERNAL       = "TRUE"']
    if projection:
        col, start = projection
        params += [
            '    "projection.enabled" = "true"',
            f'    "projection.{col}.type" = "date"',
            f'    "projection.{col}.format" = "yyyy-MM-dd"',
            f'    "projection.{col}.range" = "{start},NOW"',
            f'    "projection.{col}.interval" = "1"',
            f'    "projection.{col}.interval.unit" = "DAYS"',
            f'    "storage.location.template" = "s3://${{aws_s3_bucket.lake.bucket}}/{location}{col}=$${{{col}}}/"',
        ]
    part = f'\n  partition_keys {{\n    name = "{partition}"\n    type = "string"\n  }}\n' if partition else ""
    return f'''resource "aws_glue_catalog_table" "{name}" {{
  name          = "{name}"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "{comment}"
  parameters = {{
{chr(10).join(params)}
  }}
{part}
  storage_descriptor {{
    location      = "s3://${{aws_s3_bucket.lake.bucket}}/{location}"
{PARQUET if fmt == "parquet" else JSON}
{cols(columns)}
  }}
}}
'''


def schema_cols(schema):
    return [(f.name, hive(f.type)) for f in schema]


BATCH_LOG = [("source_id", "string"), ("medio", "string"), ("seccion", "string"), ("bronze_run_id", "string"),
             ("ingestion_date", "string"), ("bronze_uri", "string"), ("bronze_sha256", "string"),
             ("bronze_mode", "string"), ("silver_run_id", "string"), ("sequence", "int"),
             ("processed_at_utc", "string"), ("reprocessed", "boolean"), ("full_refresh", "boolean"),
             ("quarantine_uri", "string"), ("items_read", "int"), ("valid", "int"), ("invalid", "int"),
             ("duplicates_in_batch", "int"), ("new", "int"), ("updated", "int"), ("unchanged", "int"),
             ("stale", "int"), ("cross_feed_matches", "int")]
RUN_LOG = [("run_id", "string"), ("mode", "string"), ("status", "string"), ("message", "string"),
           ("finished_at_utc", "string"),
           ("sources", "array<struct<source_id:string,status:string,http_status:int,item_count:int,"
                       "error:string,s3_uri:string>>"),
           ("failed_tasks", "array<string>"), ("silver_sequence", "int"), ("silver_dirty_dates", "array<string>"),
           ("news_current", "int"), ("gold_dirty_dates", "array<string>"), ("glue_partitions_created", "int"),
           ("quality_passed", "boolean"), ("quality_failed_checks", "array<string>")]
QUALITY = [("run_id", "string"), ("check_name", "string"), ("description", "string"), ("failures", "bigint"),
           ("passed", "boolean"), ("query_id", "string"), ("bytes_scanned", "bigint"),
           ("checked_at_utc", "string")]

if __name__ == "__main__":
    out = ["# ARCHIVO GENERADO por scripts/gen_glue_tables_tf.py — no editar a mano.",
           "# Tablas con partition_keys: las particiones las registra el DAG (glue_update_catalog).",
           "# Tablas con projection.*: Athena calcula las particiones (no requieren registro).\n",
           table("silver_news", "silver/news/", "Una fila por noticia vigente",
                 schema_cols(S.NEWS), partition="pub_date"),
           table("silver_news_entities", "silver/news_entities/",
                 "Una fila por entidad distinta (persona/organizacion/lugar) por noticia vigente",
                 schema_cols(S.ENTITIES), partition="pub_date"),
           table("silver_news_key_phrases", "silver/news_key_phrases/",
                 "Una fila por frase clave distinta por noticia vigente",
                 schema_cols(S.KEY_PHRASES), partition="pub_date"),
           table("gold_news_daily", "gold/news_daily/", "Noticias por dia, medio y seccion",
                 schema_cols(S.GOLD_NEWS_DAILY), partition="pub_date"),
           table("gold_entity_daily", "gold/entity_daily/", "Noticias distintas por entidad y dia",
                 schema_cols(S.GOLD_ENTITY_DAILY), partition="pub_date"),
           table("gold_key_phrase_section_daily", "gold/key_phrase_section_daily/",
                 "Noticias distintas por frase clave, seccion y dia",
                 schema_cols(S.GOLD_KEY_PHRASE_SECTION_DAILY), partition="pub_date"),
           table("silver_news_history", "silver/news_history/", "Versiones NEW/UPDATED de cada noticia",
                 schema_cols(S.NEWS_HISTORY), partition="processed_date",
                 projection=("processed_date", "2026-01-01")),
           table("silver_nlp_analysis", "silver/nlp_analysis/", "Resultados de Comprehend por hash de texto",
                 schema_cols(S.NLP_ANALYSIS), partition="analyzed_date",
                 projection=("analyzed_date", "2026-01-01")),
           table("silver_quarantine", "quarantine/silver/", "Registros invalidos con motivo y lote Bronze",
                 schema_cols(S.QUARANTINE)),
           table("silver_batch_log", "control/silver/batches/", "Conteos por lote Bronze procesado en Silver",
                 BATCH_LOG, fmt="json"),
           table("pipeline_run_log", "control/pipeline_runs/", "Estado de cada ejecucion del DAG",
                 RUN_LOG, fmt="json"),
           table("quality_check_results", "control/quality/", "Resultados de las validaciones en Athena",
                 QUALITY, fmt="json")]
    path = os.path.join(ROOT, "terraform", "glue_tables.tf")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(out))
    print("escrito", path)
