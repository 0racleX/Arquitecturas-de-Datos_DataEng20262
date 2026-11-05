-- Misma consulta sin filtro de partición: Athena lee todas las fechas.
SELECT count(DISTINCT title) AS titulos, max(pub_date_utc) AS ultima
FROM news_datalake.silver_news;
