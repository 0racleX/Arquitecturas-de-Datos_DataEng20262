"""Capa Gold: indicadores agregados por fecha de publicación (4.4).

Tablas (Parquet, partición pub_date = fecha de publicación en UTC):

  gold_news_daily                 grano: pub_date x medio x seccion
      news_count                  noticias DISTINTAS que aparecieron en esa sección
                                  (una noticia en 2 feeds cuenta en ambas secciones)
      news_count_first_section    noticias distintas cuyo PRIMER feed fue esa sección.
                                  Suma exactamente el total de noticias del día: es la
                                  métrica para totales y conciliación.
      multi_feed_news_count       de las anteriores (news_count), cuántas están en >1 feed
      analyzed_news_count         de news_count, cuántas analizó Comprehend

  gold_entity_daily               grano: pub_date x entity_type x entity_text_norm
      news_count                  noticias DISTINTAS que mencionan la entidad
                                  (las repeticiones dentro de un texto NO inflan la cifra)
      mention_count               menciones totales (informativa)

  gold_key_phrase_section_daily   grano: pub_date x medio x seccion x key_phrase_norm
      news_count                  noticias distintas de la sección que contienen la frase

Incremental: Gold recuerda de qué versión de cada partición Silver se construyó
(`built_from`). Solo recalcula las fechas cuya versión Silver cambió.
"""
from __future__ import annotations

import logging
from collections import Counter, defaultdict

from . import schemas as S
from .bronze import sanitize_run_id
from .lake import Lake, SimulatedFailure, iso, utcnow
from .silver import LOCK_NAME, POINTER_KEY as SILVER_POINTER, QualityGateError, part_key as silver_key

log = logging.getLogger(__name__)
GOLD_POINTER = "control/gold/current.json"


def gold_key(table: str, date: str, version: str) -> str:
    return f"gold/{table}/pub_date={date}/v={version}/part-00000.parquet"


def build_partition(news: list[dict], ents: list[dict], kps: list[dict], version: str, now) -> dict:
    meta = {"gold_version": version, "built_at_utc": now}

    daily = defaultdict(lambda: {"ids": set(), "first": set(), "multi": set(), "analyzed": set()})
    for n in news:
        for sec in dict.fromkeys(n["secciones"] or [n["first_seccion"]]):
            g = daily[(n["medio"], sec)]
            g["ids"].add(n["news_id"])
            if n["n_sources"] > 1:
                g["multi"].add(n["news_id"])
            if n["nlp_status"] in ("ANALYZED", "ANALYZED_TRUNCATED"):
                g["analyzed"].add(n["news_id"])
        daily[(n["medio"], n["first_seccion"])]["first"].add(n["news_id"])
    news_daily = [{"medio": m, "seccion": s, "news_count": len(g["ids"]),
                   "news_count_first_section": len(g["first"]),
                   "multi_feed_news_count": len(g["multi"]),
                   "analyzed_news_count": len(g["analyzed"]), **meta}
                  for (m, s), g in sorted(daily.items())]

    ent = defaultdict(lambda: {"ids": set(), "mentions": 0, "scores": [], "texts": Counter(), "cat": None})
    for e in ents:
        g = ent[(e["entity_type"], e["entity_text_norm"])]
        g["ids"].add(e["news_id"])
        g["mentions"] += e["mention_count"]
        g["scores"].append(e["avg_score"])
        g["texts"][e["entity_text"]] += 1
        g["cat"] = e["entity_category"]
    entity_daily = [{"entity_type": t, "entity_category": g["cat"], "entity_text_norm": n,
                     "entity_text": g["texts"].most_common(1)[0][0], "news_count": len(g["ids"]),
                     "mention_count": g["mentions"],
                     "avg_score": round(sum(g["scores"]) / len(g["scores"]), 4),
                     "max_score": max(g["scores"]), **meta}
                    for (t, n), g in sorted(ent.items())]

    kp = defaultdict(lambda: {"ids": set(), "mentions": 0, "texts": Counter()})
    for k in kps:
        for sec in dict.fromkeys(k["secciones"] or []):
            g = kp[(k["medio"], sec, k["key_phrase_norm"])]
            g["ids"].add(k["news_id"])
            g["mentions"] += k["mention_count"]
            g["texts"][k["key_phrase"]] += 1
    kp_daily = [{"medio": m, "seccion": s, "key_phrase_norm": n,
                 "key_phrase": g["texts"].most_common(1)[0][0], "news_count": len(g["ids"]),
                 "mention_count": g["mentions"], **meta}
                for (m, s, n), g in sorted(kp.items())]

    # Conciliación Silver -> Gold dentro de la partición (compuerta de calidad)
    errors = []
    if sum(r["news_count_first_section"] for r in news_daily) != len(news):
        errors.append("news_daily no concilia con silver_news")
    if sum(r["news_count"] for r in entity_daily) != len(ents):
        errors.append("entity_daily no concilia con silver_news_entities")
    if sum(r["news_count"] for r in kp_daily) != sum(len(dict.fromkeys(k["secciones"] or [])) for k in kps):
        errors.append("key_phrase_section_daily no concilia con silver_news_key_phrases")
    if errors:
        raise QualityGateError("; ".join(errors))
    return {"news_daily": news_daily, "entity_daily": entity_daily, "key_phrase_section_daily": kp_daily}


class GoldBuilder:
    def __init__(self, s3, bucket: str, now_fn=None):
        self.lake = Lake(s3, bucket)
        self.now_fn = now_fn or utcnow

    def read_pointer(self) -> dict:
        return self.lake.get_json(GOLD_POINTER) or {"sequence": 0, "run_id": None, "partitions": {},
                                                    "built_from": {}, "previous_partitions": {}}

    def build(self, run_id: str, *, simulate_failure: bool = False) -> dict:
        run_id = sanitize_run_id(run_id)
        silver = self.lake.get_json(SILVER_POINTER) or {"partitions": {}, "sequence": 0}
        gold = self.read_pointer()
        if gold.get("run_id") == run_id and gold.get("built_from") == silver["partitions"]:
            return {**gold.get("stats", {}), "committed": True, "already_published": True}

        s_parts, built_from = silver["partitions"], gold.get("built_from", {})
        dirty = sorted(d for d, v in s_parts.items() if built_from.get(d) != v)
        removed = sorted(d for d in gold["partitions"] if d not in s_parts)
        if not dirty and not removed:
            log.info("[run_id=%s] Gold al día con Silver seq=%s", run_id, silver.get("sequence"))
            return {"run_id": run_id, "committed": False, "dirty_dates": [],
                    "silver_sequence": silver.get("sequence")}

        now = self.now_fn()
        self.lake.acquire_lock(LOCK_NAME, run_id)
        try:
            counts = Counter()
            for d in dirty:
                v = s_parts[d]
                read = lambda t: S.from_parquet_bytes(self.lake.get(silver_key(t, d, v)))  # noqa: E731
                tables = build_partition(read("news"), read("news_entities"), read("news_key_phrases"),
                                         run_id, now)
                for name, rows in tables.items():
                    self.lake.put(gold_key(name, d, run_id), S.to_parquet_bytes(rows, S.GOLD_TABLES[name]))
                    counts[name] += len(rows)
            if simulate_failure:
                raise SimulatedFailure("Fallo simulado en gold_build antes del commit")
            parts = {d: (run_id if d in dirty else gold["partitions"][d]) for d in s_parts}
            stats = {"run_id": run_id, "silver_sequence": silver.get("sequence"), "dirty_dates": dirty,
                     "removed_dates": removed, "rows_written": dict(counts), "partitions_total": len(parts)}
            self.lake.put_json(GOLD_POINTER, {
                "sequence": gold["sequence"] + 1, "run_id": run_id, "committed_at_utc": iso(now),
                "partitions": parts, "built_from": dict(s_parts),
                "previous_partitions": gold["partitions"], "stats": stats})
        finally:
            self.lake.release_lock(LOCK_NAME, run_id)
        log.info("[run_id=%s] Gold publicado: fechas recalculadas=%s", run_id, dirty)
        return {**stats, "committed": True}
