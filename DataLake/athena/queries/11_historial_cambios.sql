-- Noticias con contenido modificado: versión vigente vs. versiones anteriores.
-- text_changed = false significa que no hizo falta volver a llamar a Comprehend.
SELECT h.news_id, h.version, h.change_type, h.changed_fields, h.text_changed,
       h.title, h.pub_date_utc, h.bronze_run_id, h.processed_at_utc,
       n.version AS version_vigente
FROM news_datalake.silver_news_history h
JOIN news_datalake.silver_news n ON n.news_id = h.news_id
WHERE n.version > 1
ORDER BY h.news_id, h.version;
