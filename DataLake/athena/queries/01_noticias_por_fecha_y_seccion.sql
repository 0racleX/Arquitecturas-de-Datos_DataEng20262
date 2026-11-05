-- Consultar noticias por fecha y sección (detalle, Silver).
-- Cambie la fecha y la sección. El filtro por pub_date hace que Athena lea solo esa partición.
SELECT pub_date, medio, first_seccion AS seccion, secciones, title, link,
       pub_date_utc, nlp_status, version
FROM news_datalake.silver_news
WHERE pub_date = date_format(current_date - INTERVAL '1' DAY, '%Y-%m-%d')
  AND contains(secciones, 'Ciencia')
ORDER BY pub_date_utc DESC;
