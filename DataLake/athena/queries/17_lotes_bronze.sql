-- Lotes de Bronze: identificador de ejecución, fuente, URL, fecha UTC, estado HTTP, hash y ubicación.
SELECT ingestion_ts_utc, run_id, source_id, source_url, status, http_status,
       content_sha256, item_count, s3_uri, mode, derived_from_run_id, error
FROM news_datalake.bronze_ingestion_log
ORDER BY ingestion_ts_utc DESC
LIMIT 50;
