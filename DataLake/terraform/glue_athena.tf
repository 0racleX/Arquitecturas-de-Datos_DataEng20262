resource "aws_glue_catalog_database" "lake" {
  name        = var.glue_database_name
  description = "Data lake de noticias RSS (bronze/silver/gold)"
}

resource "aws_athena_workgroup" "lake" {
  name          = var.project
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824 # 1 GB: tope de costo por consulta
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}

# Registro de lotes de ingesta (manifiestos de Bronze), consultable desde Athena.
# Tabla sin particiones: Athena lee recursivamente control/ingestion_log/ y
# cada JSON ya trae ingestion_date y run_id como columnas.
resource "aws_glue_catalog_table" "ingestion_log" {
  name          = "bronze_ingestion_log"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json", EXTERNAL = "TRUE" }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/control/ingestion_log/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters            = { "ignore.malformed.json" = "true" }
    }

    dynamic "columns" {
      for_each = [
        ["run_id", "string"], ["source_id", "string"], ["medio", "string"], ["seccion", "string"],
        ["source_url", "string"], ["ingestion_ts_utc", "string"], ["ingestion_date", "string"],
        ["status", "string"], ["http_status", "int"], ["content_sha256", "string"],
        ["content_bytes", "bigint"], ["item_count", "int"], ["s3_uri", "string"],
        ["staging_uri", "string"], ["quarantine_uri", "string"], ["content_unchanged", "boolean"],
        ["previous_run_id", "string"], ["attempts", "int"], ["elapsed_ms", "bigint"],
        ["error", "string"], ["response_headers", "map<string,string>"], ["mode", "string"], ["derived_from_run_id", "string"],
        ["reprocess_count", "int"], ["last_reprocessed_at_utc", "string"], ["schema_version", "int"],
      ]
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}

# Consultas SQL de la entrega: aparecen en Athena -> "Saved queries" (pestaña Consultas guardadas)
resource "aws_athena_named_query" "queries" {
  for_each  = fileset("${path.module}/../athena/queries", "*.sql")
  name      = trimsuffix(each.value, ".sql")
  workgroup = aws_athena_workgroup.lake.id
  database  = aws_glue_catalog_database.lake.name
  query     = file("${path.module}/../athena/queries/${each.value}")
}
