"""Esquemas Parquet de Silver y Gold (fuente única de verdad).

`scripts/gen_glue_tables_tf.py` genera las tablas del Glue Data Catalog a partir
de este archivo, así los Parquet y el catálogo no se desalinean.

Las tablas particionadas por `pub_date` (AAAA-MM-DD, fecha de publicación en UTC)
NO llevan esa columna dentro del archivo: el valor viene de la partición.
"""
from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq

TS = pa.timestamp("ms", tz="UTC")
STR_LIST = pa.list_(pa.string())

# --------------------------------------------------------------------------- Silver
NEWS = pa.schema([
    ("news_id", pa.string()),
    ("id_strategy", pa.string()),
    ("identity_key", pa.string()),
    ("guid", pa.string()),
    ("link", pa.string()),
    ("link_canonical", pa.string()),
    ("title", pa.string()),
    ("title_raw", pa.string()),
    ("description", pa.string()),
    ("description_raw", pa.string()),
    ("pub_date_utc", TS),
    ("pub_date_raw", pa.string()),
    ("title_fingerprint", pa.string()),
    ("medio", pa.string()),
    ("first_source_id", pa.string()),
    ("first_seccion", pa.string()),
    ("source_ids", STR_LIST),
    ("secciones", STR_LIST),
    ("n_sources", pa.int32()),
    ("content_hash", pa.string()),
    ("version", pa.int32()),
    ("first_seen_at_utc", TS),
    ("last_change_at_utc", TS),
    ("first_bronze_run_id", pa.string()),
    ("first_bronze_uri", pa.string()),
    ("bronze_run_id", pa.string()),
    ("bronze_uri", pa.string()),
    ("bronze_sha256", pa.string()),
    ("text_hash", pa.string()),
    ("nlp_status", pa.string()),
    ("nlp_language", pa.string()),
    ("nlp_analyzed_at_utc", TS),
    ("silver_version", pa.string()),
    ("silver_published_at_utc", TS),
])

ENTITIES = pa.schema([
    ("news_id", pa.string()),
    ("entity_type", pa.string()),
    ("entity_category", pa.string()),
    ("entity_text", pa.string()),
    ("entity_text_norm", pa.string()),
    ("mention_count", pa.int32()),
    ("max_score", pa.float64()),
    ("avg_score", pa.float64()),
    ("text_hash", pa.string()),
    ("language", pa.string()),
    ("analyzed_at_utc", TS),
    ("pub_date_utc", TS),
    ("medio", pa.string()),
    ("first_source_id", pa.string()),
    ("secciones", STR_LIST),
    ("silver_version", pa.string()),
])

KEY_PHRASES = pa.schema([
    ("news_id", pa.string()),
    ("key_phrase", pa.string()),
    ("key_phrase_norm", pa.string()),
    ("mention_count", pa.int32()),
    ("max_score", pa.float64()),
    ("avg_score", pa.float64()),
    ("text_hash", pa.string()),
    ("language", pa.string()),
    ("analyzed_at_utc", TS),
    ("pub_date_utc", TS),
    ("medio", pa.string()),
    ("first_source_id", pa.string()),
    ("secciones", STR_LIST),
    ("silver_version", pa.string()),
])

NEWS_HISTORY = pa.schema([
    ("news_id", pa.string()),
    ("version", pa.int32()),
    ("change_type", pa.string()),
    ("changed_fields", STR_LIST),
    ("text_changed", pa.bool_()),
    ("title", pa.string()),
    ("description", pa.string()),
    ("link", pa.string()),
    ("pub_date_utc", TS),
    ("content_hash", pa.string()),
    ("previous_content_hash", pa.string()),
    ("source_id", pa.string()),
    ("bronze_run_id", pa.string()),
    ("bronze_uri", pa.string()),
    ("processed_at_utc", TS),
    ("silver_run_id", pa.string()),
])

_ENT = pa.struct([("type", pa.string()), ("text", pa.string()), ("score", pa.float64()),
                  ("begin_offset", pa.int32()), ("end_offset", pa.int32())])
_KP = pa.struct([("text", pa.string()), ("score", pa.float64()),
                 ("begin_offset", pa.int32()), ("end_offset", pa.int32())])

NLP_ANALYSIS = pa.schema([
    ("text_hash", pa.string()),
    ("analyzed_text", pa.string()),
    ("text_bytes", pa.int32()),
    ("truncated", pa.bool_()),
    ("status", pa.string()),
    ("language", pa.string()),
    ("language_score", pa.float64()),
    ("language_source", pa.string()),
    ("analyzed_at_utc", TS),
    ("analysis_version", pa.string()),
    ("entities", pa.list_(_ENT)),
    ("key_phrases", pa.list_(_KP)),
])

QUARANTINE = pa.schema([
    ("source_id", pa.string()),
    ("medio", pa.string()),
    ("seccion", pa.string()),
    ("bronze_run_id", pa.string()),
    ("ingestion_date", pa.string()),
    ("bronze_uri", pa.string()),
    ("item_index", pa.int32()),
    ("item_id_raw", pa.string()),
    ("title_raw", pa.string()),
    ("link_raw", pa.string()),
    ("description_raw", pa.string()),
    ("pub_date_raw", pa.string()),
    ("reasons", STR_LIST),
    ("reason", pa.string()),
    ("quarantined_at_utc", TS),
    ("silver_run_id", pa.string()),
])

# --------------------------------------------------------------------------- Gold
GOLD_NEWS_DAILY = pa.schema([
    ("medio", pa.string()),
    ("seccion", pa.string()),
    ("news_count", pa.int64()),
    ("news_count_first_section", pa.int64()),
    ("multi_feed_news_count", pa.int64()),
    ("analyzed_news_count", pa.int64()),
    ("gold_version", pa.string()),
    ("built_at_utc", TS),
])

GOLD_ENTITY_DAILY = pa.schema([
    ("entity_type", pa.string()),
    ("entity_category", pa.string()),
    ("entity_text_norm", pa.string()),
    ("entity_text", pa.string()),
    ("news_count", pa.int64()),
    ("mention_count", pa.int64()),
    ("avg_score", pa.float64()),
    ("max_score", pa.float64()),
    ("gold_version", pa.string()),
    ("built_at_utc", TS),
])

GOLD_KEY_PHRASE_SECTION_DAILY = pa.schema([
    ("medio", pa.string()),
    ("seccion", pa.string()),
    ("key_phrase_norm", pa.string()),
    ("key_phrase", pa.string()),
    ("news_count", pa.int64()),
    ("mention_count", pa.int64()),
    ("gold_version", pa.string()),
    ("built_at_utc", TS),
])

SILVER_TABLES = {"news": NEWS, "news_entities": ENTITIES, "news_key_phrases": KEY_PHRASES}
GOLD_TABLES = {"news_daily": GOLD_NEWS_DAILY, "entity_daily": GOLD_ENTITY_DAILY,
               "key_phrase_section_daily": GOLD_KEY_PHRASE_SECTION_DAILY}


def to_parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    names = set(schema.names)
    table = pa.Table.from_pylist([{k: v for k, v in r.items() if k in names} for r in rows],
                                 schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def from_parquet_bytes(data: bytes) -> list[dict]:
    return pq.read_table(io.BytesIO(data)).to_pylist()
