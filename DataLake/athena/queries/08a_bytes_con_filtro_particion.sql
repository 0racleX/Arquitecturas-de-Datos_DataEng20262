-- Ejecute esta y la 08b, y compare "Data scanned" en el panel de resultados.
-- Con filtro de partición Athena solo lee los archivos de esa fecha.
SELECT count(DISTINCT title) AS titulos, max(pub_date_utc) AS ultima
FROM news_datalake.silver_news
WHERE pub_date = (SELECT max(pub_date) FROM news_datalake.gold_news_daily);
