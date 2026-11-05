-- Conteos de entrada y resultado por lote Bronze procesado en Silver.
-- recibidos = invalidos + repetidos_en_lote + nuevos + actualizados + sin_cambio + obsoletos
SELECT processed_at_utc, source_id, bronze_run_id, bronze_mode, reprocessed,
       items_read AS recibidos, invalid AS invalidos, duplicates_in_batch AS repetidos_en_lote,
       "new" AS nuevos, updated AS actualizados, unchanged AS sin_cambio, stale AS obsoletos,
       cross_feed_matches AS en_otro_feed,
       items_read = invalid + duplicates_in_batch + "new" + updated + unchanged + stale AS cuadra
FROM news_datalake.silver_batch_log
ORDER BY processed_at_utc DESC, source_id
LIMIT 50;
