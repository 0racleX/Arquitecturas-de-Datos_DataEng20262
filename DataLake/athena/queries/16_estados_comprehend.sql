-- Tratamiento explícito de textos vacíos, extensos o en idiomas no contemplados.
SELECT nlp_status, nlp_language, count(*) AS noticias
FROM news_datalake.silver_news
GROUP BY nlp_status, nlp_language
ORDER BY noticias DESC;
