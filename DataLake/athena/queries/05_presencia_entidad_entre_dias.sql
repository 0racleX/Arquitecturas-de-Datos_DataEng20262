-- Pregunta 4: ¿Cómo cambia la presencia de una entidad entre fechas?
-- Por defecto toma la entidad con más noticias. Para elegir otra, cambie el WITH por ejemplo por:
--   SELECT 'PERSON' AS tipo, 'gustavo petro' AS norm
WITH objetivo AS (
  SELECT entity_type AS tipo, entity_text_norm AS norm FROM news_datalake.gold_entity_daily
  GROUP BY entity_type, entity_text_norm ORDER BY sum(news_count) DESC LIMIT 1
)
SELECT pub_date, entity_type, entity_text, news_count,
       lag(news_count) OVER (ORDER BY pub_date)              AS noticias_dia_anterior,
       news_count - lag(news_count) OVER (ORDER BY pub_date) AS variacion
FROM news_datalake.gold_entity_daily g
JOIN objetivo o ON g.entity_type = o.tipo AND g.entity_text_norm = o.norm
ORDER BY pub_date;
