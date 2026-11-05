-- Pregunta 3: ¿Qué frases clave son más frecuentes por sección? (top 10 por sección)
WITH t AS (
  SELECT seccion, key_phrase_norm, max_by(key_phrase, news_count) AS frase, sum(news_count) AS noticias
  FROM news_datalake.gold_key_phrase_section_daily
  GROUP BY seccion, key_phrase_norm
)
SELECT seccion, frase, noticias
FROM (SELECT t.*, row_number() OVER (PARTITION BY seccion ORDER BY noticias DESC, frase) AS rk FROM t)
WHERE rk <= 10
ORDER BY seccion, noticias DESC;
