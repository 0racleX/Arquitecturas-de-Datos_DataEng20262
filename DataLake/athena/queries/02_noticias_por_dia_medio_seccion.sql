-- Pregunta 1: ¿Cuántas noticias se publican por día, medio y sección? (Gold)
-- news_count: noticias distintas que aparecieron en la sección (una nota en 2 feeds cuenta en ambas).
-- news_count_first_section: cada noticia cuenta una sola vez (en su primer feed); su suma = total del día.
SELECT pub_date, medio, seccion, news_count, news_count_first_section, multi_feed_news_count
FROM news_datalake.gold_news_daily
WHERE pub_date >= date_format(current_date - INTERVAL '14' DAY, '%Y-%m-%d')
ORDER BY pub_date DESC, medio, seccion;
