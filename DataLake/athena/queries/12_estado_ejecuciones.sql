-- Estado de cada ejecución del DAG: COMPLETE, PARTIAL o FAILED, con el detalle por fuente.
SELECT finished_at_utc, run_id, mode, status, message, failed_tasks,
       transform(sources, s -> s.source_id || ':' || s.status) AS fuentes,
       news_current, silver_dirty_dates, quality_passed
FROM news_datalake.pipeline_run_log
ORDER BY finished_at_utc DESC
LIMIT 30;
