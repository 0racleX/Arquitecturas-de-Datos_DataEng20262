-- Conciliar una agregación de Gold contra Silver: noticias por día y medio.
-- Debe devolver diferencia = 0 en todas las filas.
WITH silver AS (
  SELECT pub_date, medio, count(*) AS noticias_silver
  FROM news_datalake.silver_news GROUP BY pub_date, medio
), gold AS (
  SELECT pub_date, medio, sum(news_count_first_section) AS noticias_gold
  FROM news_datalake.gold_news_daily GROUP BY pub_date, medio
)
SELECT coalesce(s.pub_date, g.pub_date) AS pub_date, coalesce(s.medio, g.medio) AS medio,
       s.noticias_silver, g.noticias_gold,
       coalesce(s.noticias_silver, 0) - coalesce(g.noticias_gold, 0) AS diferencia
FROM silver s FULL OUTER JOIN gold g ON s.pub_date = g.pub_date AND s.medio = g.medio
ORDER BY 1 DESC;
