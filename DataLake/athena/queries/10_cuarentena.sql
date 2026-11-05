-- Registros rechazados con su motivo y trazabilidad hasta el archivo y lote de Bronze.
SELECT quarantined_at_utc, source_id, bronze_run_id, item_index, reason,
       title_raw, link_raw, pub_date_raw, bronze_uri
FROM news_datalake.silver_quarantine
ORDER BY quarantined_at_utc DESC
LIMIT 50;
