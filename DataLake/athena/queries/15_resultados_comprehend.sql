-- Resultados de Comprehend: texto analizado, hash, idioma, fecha y puntuaciones de confianza.
SELECT n.title, a.status, a.language, a.language_score, a.truncated, a.analyzed_at_utc,
       a.text_hash, e.type AS tipo, e.text AS entidad, e.score AS confianza
FROM news_datalake.silver_nlp_analysis a
JOIN news_datalake.silver_news n ON n.text_hash = a.text_hash
CROSS JOIN UNNEST(a.entities) AS t (e)
WHERE e.type IN ('PERSON', 'ORGANIZATION', 'LOCATION')
ORDER BY a.analyzed_at_utc DESC, n.title
LIMIT 50;
