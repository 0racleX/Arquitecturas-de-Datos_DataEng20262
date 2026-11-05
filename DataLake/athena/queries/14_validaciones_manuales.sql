-- Validaciones de calidad en una sola consulta (todas deben dar 0).
SELECT
  (SELECT count(*) - count(DISTINCT news_id) FROM news_datalake.silver_news) AS ids_duplicados,
  (SELECT count(*) FROM news_datalake.silver_news
    WHERE title IS NULL OR link IS NULL OR pub_date_utc IS NULL)          AS sin_obligatorios,
  (SELECT count(*) FROM news_datalake.silver_news
    WHERE NOT regexp_like(link, '^https?://')
       OR date_format(pub_date_utc, '%Y-%m-%d') <> pub_date)               AS fecha_o_enlace_invalido,
  (SELECT count(*) FROM news_datalake.silver_news_entities e
    LEFT JOIN news_datalake.silver_news n ON e.news_id = n.news_id AND e.pub_date = n.pub_date
    WHERE n.news_id IS NULL)                                               AS entidades_huerfanas,
  (SELECT count(*) FROM news_datalake.silver_news_key_phrases k
    LEFT JOIN news_datalake.silver_news n ON k.news_id = n.news_id AND k.pub_date = n.pub_date
    WHERE n.news_id IS NULL)                                               AS frases_huerfanas;
