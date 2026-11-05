# ARCHIVO GENERADO por scripts/gen_glue_tables_tf.py — no editar a mano.
# Tablas con partition_keys: las particiones las registra el DAG (glue_update_catalog).
# Tablas con projection.*: Athena calcula las particiones (no requieren registro).

resource "aws_glue_catalog_table" "silver_news" {
  name          = "silver_news"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Una fila por noticia vigente"
  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  partition_keys {
    name = "pub_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/silver/news/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "news_id"
      type = "string"
    }
    columns {
      name = "id_strategy"
      type = "string"
    }
    columns {
      name = "identity_key"
      type = "string"
    }
    columns {
      name = "guid"
      type = "string"
    }
    columns {
      name = "link"
      type = "string"
    }
    columns {
      name = "link_canonical"
      type = "string"
    }
    columns {
      name = "title"
      type = "string"
    }
    columns {
      name = "title_raw"
      type = "string"
    }
    columns {
      name = "description"
      type = "string"
    }
    columns {
      name = "description_raw"
      type = "string"
    }
    columns {
      name = "pub_date_utc"
      type = "timestamp"
    }
    columns {
      name = "pub_date_raw"
      type = "string"
    }
    columns {
      name = "title_fingerprint"
      type = "string"
    }
    columns {
      name = "medio"
      type = "string"
    }
    columns {
      name = "first_source_id"
      type = "string"
    }
    columns {
      name = "first_seccion"
      type = "string"
    }
    columns {
      name = "source_ids"
      type = "array<string>"
    }
    columns {
      name = "secciones"
      type = "array<string>"
    }
    columns {
      name = "n_sources"
      type = "int"
    }
    columns {
      name = "content_hash"
      type = "string"
    }
    columns {
      name = "version"
      type = "int"
    }
    columns {
      name = "first_seen_at_utc"
      type = "timestamp"
    }
    columns {
      name = "last_change_at_utc"
      type = "timestamp"
    }
    columns {
      name = "first_bronze_run_id"
      type = "string"
    }
    columns {
      name = "first_bronze_uri"
      type = "string"
    }
    columns {
      name = "bronze_run_id"
      type = "string"
    }
    columns {
      name = "bronze_uri"
      type = "string"
    }
    columns {
      name = "bronze_sha256"
      type = "string"
    }
    columns {
      name = "text_hash"
      type = "string"
    }
    columns {
      name = "nlp_status"
      type = "string"
    }
    columns {
      name = "nlp_language"
      type = "string"
    }
    columns {
      name = "nlp_analyzed_at_utc"
      type = "timestamp"
    }
    columns {
      name = "silver_version"
      type = "string"
    }
    columns {
      name = "silver_published_at_utc"
      type = "timestamp"
    }
  }
}

resource "aws_glue_catalog_table" "silver_news_entities" {
  name          = "silver_news_entities"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Una fila por entidad distinta (persona/organizacion/lugar) por noticia vigente"
  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  partition_keys {
    name = "pub_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/silver/news_entities/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "news_id"
      type = "string"
    }
    columns {
      name = "entity_type"
      type = "string"
    }
    columns {
      name = "entity_category"
      type = "string"
    }
    columns {
      name = "entity_text"
      type = "string"
    }
    columns {
      name = "entity_text_norm"
      type = "string"
    }
    columns {
      name = "mention_count"
      type = "int"
    }
    columns {
      name = "max_score"
      type = "double"
    }
    columns {
      name = "avg_score"
      type = "double"
    }
    columns {
      name = "text_hash"
      type = "string"
    }
    columns {
      name = "language"
      type = "string"
    }
    columns {
      name = "analyzed_at_utc"
      type = "timestamp"
    }
    columns {
      name = "pub_date_utc"
      type = "timestamp"
    }
    columns {
      name = "medio"
      type = "string"
    }
    columns {
      name = "first_source_id"
      type = "string"
    }
    columns {
      name = "secciones"
      type = "array<string>"
    }
    columns {
      name = "silver_version"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "silver_news_key_phrases" {
  name          = "silver_news_key_phrases"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Una fila por frase clave distinta por noticia vigente"
  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  partition_keys {
    name = "pub_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/silver/news_key_phrases/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "news_id"
      type = "string"
    }
    columns {
      name = "key_phrase"
      type = "string"
    }
    columns {
      name = "key_phrase_norm"
      type = "string"
    }
    columns {
      name = "mention_count"
      type = "int"
    }
    columns {
      name = "max_score"
      type = "double"
    }
    columns {
      name = "avg_score"
      type = "double"
    }
    columns {
      name = "text_hash"
      type = "string"
    }
    columns {
      name = "language"
      type = "string"
    }
    columns {
      name = "analyzed_at_utc"
      type = "timestamp"
    }
    columns {
      name = "pub_date_utc"
      type = "timestamp"
    }
    columns {
      name = "medio"
      type = "string"
    }
    columns {
      name = "first_source_id"
      type = "string"
    }
    columns {
      name = "secciones"
      type = "array<string>"
    }
    columns {
      name = "silver_version"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "gold_news_daily" {
  name          = "gold_news_daily"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Noticias por dia, medio y seccion"
  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  partition_keys {
    name = "pub_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/gold/news_daily/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "medio"
      type = "string"
    }
    columns {
      name = "seccion"
      type = "string"
    }
    columns {
      name = "news_count"
      type = "bigint"
    }
    columns {
      name = "news_count_first_section"
      type = "bigint"
    }
    columns {
      name = "multi_feed_news_count"
      type = "bigint"
    }
    columns {
      name = "analyzed_news_count"
      type = "bigint"
    }
    columns {
      name = "gold_version"
      type = "string"
    }
    columns {
      name = "built_at_utc"
      type = "timestamp"
    }
  }
}

resource "aws_glue_catalog_table" "gold_entity_daily" {
  name          = "gold_entity_daily"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Noticias distintas por entidad y dia"
  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  partition_keys {
    name = "pub_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/gold/entity_daily/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "entity_type"
      type = "string"
    }
    columns {
      name = "entity_category"
      type = "string"
    }
    columns {
      name = "entity_text_norm"
      type = "string"
    }
    columns {
      name = "entity_text"
      type = "string"
    }
    columns {
      name = "news_count"
      type = "bigint"
    }
    columns {
      name = "mention_count"
      type = "bigint"
    }
    columns {
      name = "avg_score"
      type = "double"
    }
    columns {
      name = "max_score"
      type = "double"
    }
    columns {
      name = "gold_version"
      type = "string"
    }
    columns {
      name = "built_at_utc"
      type = "timestamp"
    }
  }
}

resource "aws_glue_catalog_table" "gold_key_phrase_section_daily" {
  name          = "gold_key_phrase_section_daily"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Noticias distintas por frase clave, seccion y dia"
  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  partition_keys {
    name = "pub_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/gold/key_phrase_section_daily/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "medio"
      type = "string"
    }
    columns {
      name = "seccion"
      type = "string"
    }
    columns {
      name = "key_phrase_norm"
      type = "string"
    }
    columns {
      name = "key_phrase"
      type = "string"
    }
    columns {
      name = "news_count"
      type = "bigint"
    }
    columns {
      name = "mention_count"
      type = "bigint"
    }
    columns {
      name = "gold_version"
      type = "string"
    }
    columns {
      name = "built_at_utc"
      type = "timestamp"
    }
  }
}

resource "aws_glue_catalog_table" "silver_news_history" {
  name          = "silver_news_history"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Versiones NEW/UPDATED de cada noticia"
  parameters = {
    classification                            = "parquet"
    EXTERNAL                                  = "TRUE"
    "projection.enabled"                      = "true"
    "projection.processed_date.type"          = "date"
    "projection.processed_date.format"        = "yyyy-MM-dd"
    "projection.processed_date.range"         = "2026-01-01,NOW"
    "projection.processed_date.interval"      = "1"
    "projection.processed_date.interval.unit" = "DAYS"
    "storage.location.template"               = "s3://${aws_s3_bucket.lake.bucket}/silver/news_history/processed_date=$${processed_date}/"
  }

  partition_keys {
    name = "processed_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/silver/news_history/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "news_id"
      type = "string"
    }
    columns {
      name = "version"
      type = "int"
    }
    columns {
      name = "change_type"
      type = "string"
    }
    columns {
      name = "changed_fields"
      type = "array<string>"
    }
    columns {
      name = "text_changed"
      type = "boolean"
    }
    columns {
      name = "title"
      type = "string"
    }
    columns {
      name = "description"
      type = "string"
    }
    columns {
      name = "link"
      type = "string"
    }
    columns {
      name = "pub_date_utc"
      type = "timestamp"
    }
    columns {
      name = "content_hash"
      type = "string"
    }
    columns {
      name = "previous_content_hash"
      type = "string"
    }
    columns {
      name = "source_id"
      type = "string"
    }
    columns {
      name = "bronze_run_id"
      type = "string"
    }
    columns {
      name = "bronze_uri"
      type = "string"
    }
    columns {
      name = "processed_at_utc"
      type = "timestamp"
    }
    columns {
      name = "silver_run_id"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "silver_nlp_analysis" {
  name          = "silver_nlp_analysis"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Resultados de Comprehend por hash de texto"
  parameters = {
    classification                           = "parquet"
    EXTERNAL                                 = "TRUE"
    "projection.enabled"                     = "true"
    "projection.analyzed_date.type"          = "date"
    "projection.analyzed_date.format"        = "yyyy-MM-dd"
    "projection.analyzed_date.range"         = "2026-01-01,NOW"
    "projection.analyzed_date.interval"      = "1"
    "projection.analyzed_date.interval.unit" = "DAYS"
    "storage.location.template"              = "s3://${aws_s3_bucket.lake.bucket}/silver/nlp_analysis/analyzed_date=$${analyzed_date}/"
  }

  partition_keys {
    name = "analyzed_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/silver/nlp_analysis/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "text_hash"
      type = "string"
    }
    columns {
      name = "analyzed_text"
      type = "string"
    }
    columns {
      name = "text_bytes"
      type = "int"
    }
    columns {
      name = "truncated"
      type = "boolean"
    }
    columns {
      name = "status"
      type = "string"
    }
    columns {
      name = "language"
      type = "string"
    }
    columns {
      name = "language_score"
      type = "double"
    }
    columns {
      name = "language_source"
      type = "string"
    }
    columns {
      name = "analyzed_at_utc"
      type = "timestamp"
    }
    columns {
      name = "analysis_version"
      type = "string"
    }
    columns {
      name = "entities"
      type = "array<struct<type:string,text:string,score:double,begin_offset:int,end_offset:int>>"
    }
    columns {
      name = "key_phrases"
      type = "array<struct<text:string,score:double,begin_offset:int,end_offset:int>>"
    }
  }
}

resource "aws_glue_catalog_table" "silver_quarantine" {
  name          = "silver_quarantine"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Registros invalidos con motivo y lote Bronze"
  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/quarantine/silver/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "source_id"
      type = "string"
    }
    columns {
      name = "medio"
      type = "string"
    }
    columns {
      name = "seccion"
      type = "string"
    }
    columns {
      name = "bronze_run_id"
      type = "string"
    }
    columns {
      name = "ingestion_date"
      type = "string"
    }
    columns {
      name = "bronze_uri"
      type = "string"
    }
    columns {
      name = "item_index"
      type = "int"
    }
    columns {
      name = "item_id_raw"
      type = "string"
    }
    columns {
      name = "title_raw"
      type = "string"
    }
    columns {
      name = "link_raw"
      type = "string"
    }
    columns {
      name = "description_raw"
      type = "string"
    }
    columns {
      name = "pub_date_raw"
      type = "string"
    }
    columns {
      name = "reasons"
      type = "array<string>"
    }
    columns {
      name = "reason"
      type = "string"
    }
    columns {
      name = "quarantined_at_utc"
      type = "timestamp"
    }
    columns {
      name = "silver_run_id"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "silver_batch_log" {
  name          = "silver_batch_log"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Conteos por lote Bronze procesado en Silver"
  parameters = {
    classification = "json"
    EXTERNAL       = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/control/silver/batches/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters            = { "ignore.malformed.json" = "true" }
    }
    columns {
      name = "source_id"
      type = "string"
    }
    columns {
      name = "medio"
      type = "string"
    }
    columns {
      name = "seccion"
      type = "string"
    }
    columns {
      name = "bronze_run_id"
      type = "string"
    }
    columns {
      name = "ingestion_date"
      type = "string"
    }
    columns {
      name = "bronze_uri"
      type = "string"
    }
    columns {
      name = "bronze_sha256"
      type = "string"
    }
    columns {
      name = "bronze_mode"
      type = "string"
    }
    columns {
      name = "silver_run_id"
      type = "string"
    }
    columns {
      name = "sequence"
      type = "int"
    }
    columns {
      name = "processed_at_utc"
      type = "string"
    }
    columns {
      name = "reprocessed"
      type = "boolean"
    }
    columns {
      name = "full_refresh"
      type = "boolean"
    }
    columns {
      name = "quarantine_uri"
      type = "string"
    }
    columns {
      name = "items_read"
      type = "int"
    }
    columns {
      name = "valid"
      type = "int"
    }
    columns {
      name = "invalid"
      type = "int"
    }
    columns {
      name = "duplicates_in_batch"
      type = "int"
    }
    columns {
      name = "new"
      type = "int"
    }
    columns {
      name = "updated"
      type = "int"
    }
    columns {
      name = "unchanged"
      type = "int"
    }
    columns {
      name = "stale"
      type = "int"
    }
    columns {
      name = "cross_feed_matches"
      type = "int"
    }
  }
}

resource "aws_glue_catalog_table" "pipeline_run_log" {
  name          = "pipeline_run_log"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Estado de cada ejecucion del DAG"
  parameters = {
    classification = "json"
    EXTERNAL       = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/control/pipeline_runs/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters            = { "ignore.malformed.json" = "true" }
    }
    columns {
      name = "run_id"
      type = "string"
    }
    columns {
      name = "mode"
      type = "string"
    }
    columns {
      name = "status"
      type = "string"
    }
    columns {
      name = "message"
      type = "string"
    }
    columns {
      name = "finished_at_utc"
      type = "string"
    }
    columns {
      name = "sources"
      type = "array<struct<source_id:string,status:string,http_status:int,item_count:int,error:string,s3_uri:string>>"
    }
    columns {
      name = "failed_tasks"
      type = "array<string>"
    }
    columns {
      name = "silver_sequence"
      type = "int"
    }
    columns {
      name = "silver_dirty_dates"
      type = "array<string>"
    }
    columns {
      name = "news_current"
      type = "int"
    }
    columns {
      name = "gold_dirty_dates"
      type = "array<string>"
    }
    columns {
      name = "glue_partitions_created"
      type = "int"
    }
    columns {
      name = "quality_passed"
      type = "boolean"
    }
    columns {
      name = "quality_failed_checks"
      type = "array<string>"
    }
  }
}

resource "aws_glue_catalog_table" "quality_check_results" {
  name          = "quality_check_results"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Resultados de las validaciones en Athena"
  parameters = {
    classification = "json"
    EXTERNAL       = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/control/quality/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters            = { "ignore.malformed.json" = "true" }
    }
    columns {
      name = "run_id"
      type = "string"
    }
    columns {
      name = "check_name"
      type = "string"
    }
    columns {
      name = "description"
      type = "string"
    }
    columns {
      name = "failures"
      type = "bigint"
    }
    columns {
      name = "passed"
      type = "boolean"
    }
    columns {
      name = "query_id"
      type = "string"
    }
    columns {
      name = "bytes_scanned"
      type = "bigint"
    }
    columns {
      name = "checked_at_utc"
      type = "string"
    }
  }
}
