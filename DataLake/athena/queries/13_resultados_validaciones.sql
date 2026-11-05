-- Resultado de las validaciones de calidad que ejecuta el DAG (paso athena_validate).
SELECT checked_at_utc, run_id, check_name, description, failures, passed
FROM news_datalake.quality_check_results
WHERE run_id = (SELECT max_by(run_id, checked_at_utc) FROM news_datalake.quality_check_results)
ORDER BY check_name;
