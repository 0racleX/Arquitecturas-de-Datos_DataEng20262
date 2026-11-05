"""Validaciones finales con Athena (paso 8 del DAG).

Cada chequeo es una consulta que devuelve una columna `failures`; 0 = OK.
Los resultados se guardan como JSON Lines en
control/quality/check_date=AAAA-MM-DD/run_id=<id>.json  (tabla quality_check_results).
Además se mide el efecto del particionamiento: la misma consulta con y sin
filtro de partición, registrando los bytes escaneados de cada una.
"""
from __future__ import annotations

import json
import logging
import time

from .lake import Lake, iso, utcnow

log = logging.getLogger(__name__)

CHECKS: list[tuple[str, str, str]] = [
    ("news_id_unico", "Unicidad del identificador de noticia en el conjunto vigente",
     "SELECT count(*) - count(DISTINCT news_id) AS failures FROM {db}.silver_news"),
    ("campos_obligatorios", "Presencia de campos obligatorios",
     "SELECT count(*) AS failures FROM {db}.silver_news WHERE news_id IS NULL OR title IS NULL "
     "OR trim(title) = '' OR link IS NULL OR pub_date_utc IS NULL OR medio IS NULL OR first_seccion IS NULL"),
    ("fechas_validas", "Fecha de publicación válida y coherente con la partición",
     "SELECT count(*) AS failures FROM {db}.silver_news "
     "WHERE pub_date_utc > CAST(current_timestamp AS timestamp) + INTERVAL '1' DAY "
     "OR pub_date_utc < TIMESTAMP '2000-01-01 00:00:00' "
     "OR date_format(pub_date_utc, '%Y-%m-%d') <> pub_date"),
    ("enlaces_validos", "Enlace http(s) válido",
     "SELECT count(*) AS failures FROM {db}.silver_news WHERE NOT regexp_like(link, '^https?://[^/\\s]+')"),
    ("entidades_sin_noticia", "Ausencia de entidades sin noticia asociada",
     "SELECT count(*) AS failures FROM {db}.silver_news_entities e LEFT JOIN {db}.silver_news n "
     "ON e.news_id = n.news_id AND e.pub_date = n.pub_date WHERE n.news_id IS NULL"),
    ("frases_sin_noticia", "Ausencia de frases clave sin noticia asociada",
     "SELECT count(*) AS failures FROM {db}.silver_news_key_phrases k LEFT JOIN {db}.silver_news n "
     "ON k.news_id = n.news_id AND k.pub_date = n.pub_date WHERE n.news_id IS NULL"),
    ("entidad_distinta_por_noticia", "Una fila por entidad distinta en cada noticia",
     "SELECT count(*) AS failures FROM (SELECT news_id, entity_type, entity_text_norm "
     "FROM {db}.silver_news_entities GROUP BY 1, 2, 3 HAVING count(*) > 1)"),
    ("conciliacion_noticias", "Conciliación gold_news_daily vs silver_news (por día, medio y sección)",
     "WITH s AS (SELECT pub_date, medio, first_seccion AS seccion, count(*) AS n FROM {db}.silver_news "
     "GROUP BY 1, 2, 3), g AS (SELECT pub_date, medio, seccion, news_count_first_section AS n "
     "FROM {db}.gold_news_daily WHERE news_count_first_section > 0) "
     "SELECT count(*) AS failures FROM s FULL OUTER JOIN g ON s.pub_date = g.pub_date "
     "AND s.medio = g.medio AND s.seccion = g.seccion WHERE coalesce(s.n, -1) <> coalesce(g.n, -1)"),
    ("conciliacion_entidades", "Conciliación gold_entity_daily vs silver_news_entities",
     "WITH s AS (SELECT pub_date, entity_type, entity_text_norm, count(DISTINCT news_id) AS n "
     "FROM {db}.silver_news_entities GROUP BY 1, 2, 3), g AS (SELECT pub_date, entity_type, "
     "entity_text_norm, news_count AS n FROM {db}.gold_entity_daily) "
     "SELECT count(*) AS failures FROM s FULL OUTER JOIN g ON s.pub_date = g.pub_date "
     "AND s.entity_type = g.entity_type AND s.entity_text_norm = g.entity_text_norm "
     "WHERE coalesce(s.n, -1) <> coalesce(g.n, -1)"),
    ("conciliacion_frases", "Conciliación gold_key_phrase_section_daily vs silver_news_key_phrases",
     "WITH s AS (SELECT k.pub_date, k.medio, sec AS seccion, k.key_phrase_norm, count(DISTINCT k.news_id) AS n "
     "FROM {db}.silver_news_key_phrases k CROSS JOIN UNNEST(array_distinct(k.secciones)) AS t(sec) "
     "GROUP BY 1, 2, 3, 4), g AS (SELECT pub_date, medio, seccion, key_phrase_norm, news_count AS n "
     "FROM {db}.gold_key_phrase_section_daily) SELECT count(*) AS failures FROM s FULL OUTER JOIN g "
     "ON s.pub_date = g.pub_date AND s.medio = g.medio AND s.seccion = g.seccion "
     "AND s.key_phrase_norm = g.key_phrase_norm WHERE coalesce(s.n, -1) <> coalesce(g.n, -1)"),
    ("cuarentena_trazable", "Todo rechazo tiene motivo y referencia a Bronze",
     "SELECT count(*) AS failures FROM {db}.silver_quarantine "
     "WHERE reason IS NULL OR reason = '' OR bronze_uri IS NULL OR bronze_run_id IS NULL"),
    ("conteos_por_lote_cuadran", "Por lote: recibidos = inválidos + repetidos + nuevos + actualizados + sin cambio + obsoletos",
     "SELECT count(*) AS failures FROM {db}.silver_batch_log WHERE items_read <> invalid + "
     "duplicates_in_batch + \"new\" + updated + unchanged + stale"),
]

PRUNING_SQL = {
    "con_filtro_particion": "SELECT count(DISTINCT title) AS n FROM {db}.silver_news WHERE pub_date = '{date}'",
    "sin_filtro_particion": "SELECT count(DISTINCT title) AS n FROM {db}.silver_news",
}


class AthenaRunner:
    def __init__(self, athena, workgroup: str, database: str, poll_seconds: float = 1.5,
                 timeout_seconds: int = 300):
        self.athena, self.workgroup, self.database = athena, workgroup, database
        self.poll, self.timeout = poll_seconds, timeout_seconds

    def run(self, sql: str) -> dict:
        qid = self.athena.start_query_execution(
            QueryString=sql, WorkGroup=self.workgroup,
            QueryExecutionContext={"Database": self.database})["QueryExecutionId"]
        deadline = time.monotonic() + self.timeout
        while True:
            q = self.athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = q["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.monotonic() > deadline:
                self.athena.stop_query_execution(QueryExecutionId=qid)
                raise TimeoutError(f"Athena {qid} superó {self.timeout}s")
            time.sleep(self.poll)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena {qid} {state}: {q['Status'].get('StateChangeReason')}\n{sql}")
        rs = self.athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        header = [c.get("VarCharValue") for c in rs[0]["Data"]] if rs else []
        rows = [dict(zip(header, [c.get("VarCharValue") for c in r["Data"]])) for r in rs[1:]]
        return {"query_id": qid, "rows": rows,
                "bytes_scanned": q.get("Statistics", {}).get("DataScannedInBytes", 0)}


def run_checks(runner, lake: Lake, run_id: str, latest_date: str | None) -> dict:
    db = runner.database
    now = utcnow()
    results = []
    for name, desc, sql in CHECKS:
        r = runner.run(sql.format(db=db))
        failures = int((r["rows"][0].get("failures") if r["rows"] else 0) or 0)
        results.append({"run_id": run_id, "check_name": name, "description": desc,
                        "failures": failures, "passed": failures == 0, "query_id": r["query_id"],
                        "bytes_scanned": r["bytes_scanned"], "checked_at_utc": iso(now)})
        log.info("[run_id=%s check=%s] failures=%s", run_id, name, failures)

    pruning = {}
    if latest_date:
        for label, sql in PRUNING_SQL.items():
            r = runner.run(sql.format(db=db, date=latest_date))
            pruning[label] = {"bytes_scanned": r["bytes_scanned"], "query_id": r["query_id"]}
        log.info("[run_id=%s] bytes escaneados con/sin filtro de partición: %s", run_id, pruning)

    lake.put(f"control/quality/check_date={now.date()}/run_id={run_id}.json",
             "\n".join(json.dumps(r) for r in results).encode(), "application/json")
    failed = [r["check_name"] for r in results if not r["passed"]]
    return {"passed": not failed, "failed_checks": failed, "checks": len(results),
            "pruning": pruning, "results": results}
