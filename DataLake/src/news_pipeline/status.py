"""Registro del estado del procesamiento (paso 9 del DAG).

COMPLETE  todas las fuentes respondieron bien, todos los pasos terminaron y las
          validaciones de Athena pasaron.
PARTIAL   al menos una fuente falló (HTTP, red, XML), pero las demás se publicaron
          y las validaciones pasaron. Una fuente caída NUNCA se registra como
          "fuente sin noticias": queda con su estado HTTP y su error.
FAILED    falló un paso del pipeline (validación inicial, Silver, Gold, Glue o
          Athena), fallaron todas las fuentes, o una validación de calidad no pasó.

Se guarda en control/pipeline_runs/run_date=AAAA-MM-DD/run_id=<id>.json
(tabla pipeline_run_log).
"""
from __future__ import annotations

import json

from .bronze import BronzeStore, sanitize_run_id
from .lake import Lake, iso, utcnow

COMPLETE, PARTIAL, FAILED = "COMPLETE", "PARTIAL", "FAILED"


def source_results(store: BronzeStore, run_id: str, source_ids: list[str]) -> list[dict]:
    out = []
    for sid in source_ids:
        try:
            m = store.find_manifest(sid, sanitize_run_id(run_id))
            out.append({"source_id": sid, "status": m.status, "http_status": m.http_status,
                        "item_count": m.item_count, "error": m.error, "s3_uri": m.s3_uri})
        except Exception:
            out.append({"source_id": sid, "status": "NOT_RUN", "http_status": None,
                        "item_count": 0, "error": "La tarea de extracción no registró un lote", "s3_uri": None})
    return out


def decide(mode: str, sources: list[dict], failed_tasks: list[str], quality: dict | None) -> tuple[str, str]:
    if failed_tasks:
        return FAILED, f"Tareas fallidas: {', '.join(failed_tasks)}"
    if quality is not None and not quality.get("passed", False):
        return FAILED, f"Validaciones fallidas: {', '.join(quality.get('failed_checks', []))}"
    if mode == "extract":
        bad = [s["source_id"] for s in sources if s["status"] != "SUCCESS"]
        if bad and len(bad) == len(sources):
            return FAILED, "Ninguna fuente respondió correctamente"
        if bad:
            return PARTIAL, f"Fuentes con error (no se interpretan como 'sin noticias'): {', '.join(bad)}"
    return COMPLETE, "Ejecución completa"


def record(lake: Lake, *, run_id: str, mode: str, sources: list[dict], failed_tasks: list[str],
           silver: dict | None, gold: dict | None, catalog: dict | None, quality: dict | None) -> dict:
    status, message = decide(mode, sources, failed_tasks, quality)
    now = utcnow()
    rec = {"run_id": run_id, "mode": mode, "status": status, "message": message,
           "finished_at_utc": iso(now), "sources": sources, "failed_tasks": failed_tasks,
           "silver_sequence": (silver or {}).get("sequence"),
           "silver_dirty_dates": (silver or {}).get("dirty_dates", []),
           "news_current": (silver or {}).get("news_current"),
           "gold_dirty_dates": (gold or {}).get("dirty_dates", []),
           "glue_partitions_created": sum(len(v.get("created", [])) for v in (catalog or {}).values()
                                          if isinstance(v, dict)),
           "quality_passed": (quality or {}).get("passed"),
           "quality_failed_checks": (quality or {}).get("failed_checks", [])}
    lake.put(f"control/pipeline_runs/run_date={now.date()}/run_id={sanitize_run_id(run_id)}.json",
             json.dumps(rec, ensure_ascii=False, default=str).encode(), "application/json")
    return rec
