-- Limitación documentada: la misma nota publicada con URLs distintas queda como dos news_id.
-- Esta consulta agrupa por título normalizado para revisarlas (no se fusionan automáticamente).
SELECT title_fingerprint, count(*) AS noticias, array_agg(title) AS titulos, array_agg(link) AS enlaces
FROM news_datalake.silver_news
GROUP BY title_fingerprint
HAVING count(*) > 1;
