-- Las diez organizaciones presentes en más noticias (Gold).
-- news_count cuenta noticias DISTINTAS: repetir el nombre dentro de un texto no infla el indicador.
-- Sumar entre días es válido porque cada noticia tiene una sola fecha de publicación.
SELECT max_by(entity_text, news_count) AS organizacion,
       sum(news_count)                 AS noticias,
       sum(mention_count)              AS menciones
FROM news_datalake.gold_entity_daily
WHERE entity_type = 'ORGANIZATION'
GROUP BY entity_text_norm
ORDER BY noticias DESC, organizacion
LIMIT 10;
