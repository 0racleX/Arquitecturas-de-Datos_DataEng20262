-- Pregunta 2: ¿Qué personas, organizaciones y lugares aparecen en más noticias? (top 10 por categoría)
WITH t AS (
  SELECT entity_category, entity_text_norm,
         max_by(entity_text, news_count) AS entidad, sum(news_count) AS noticias
  FROM news_datalake.gold_entity_daily
  GROUP BY entity_category, entity_text_norm
)
SELECT entity_category, entidad, noticias
FROM (SELECT t.*, row_number() OVER (PARTITION BY entity_category ORDER BY noticias DESC, entidad) AS rk FROM t)
WHERE rk <= 10
ORDER BY entity_category, noticias DESC;
