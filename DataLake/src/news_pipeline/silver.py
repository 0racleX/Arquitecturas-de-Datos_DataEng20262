"""Capa Silver: limpieza, validación, deduplicación, control de cambios,
enriquecimiento con Comprehend y publicación particionada por fecha (4.3).

Tres pasos, cada uno idempotente para el mismo run_id (son tareas del DAG):

  prepare(run_id)  Lee los lotes Bronze pendientes, limpia, valida, deduplica y
                   fusiona con el estado vigente. Deja el resultado en
                   work/silver/run_id=<id>/ (no visible en Athena).
  enrich(run_id)   Llama a Comprehend SOLO para los textos cuyo hash no está en
                   la caché silver/nlp_analysis/.
  publish(run_id)  Calcula entidades/frases clave, pasa la compuerta de calidad,
                   reescribe solo las particiones de fecha que cambiaron y hace
                   commit moviendo el puntero control/silver/current.json.

Rutas finales:
  silver/news/pub_date=D/v=<run>/part-00000.parquet               (tabla silver_news)
  silver/news_entities/pub_date=D/v=<run>/part-00000.parquet      (silver_news_entities)
  silver/news_key_phrases/pub_date=D/v=<run>/part-00000.parquet   (silver_news_key_phrases)
  silver/news_history/processed_date=D/part-<run>.parquet         (silver_news_history)
  silver/nlp_analysis/analyzed_date=D/part-<run>-<hash>.parquet   (silver_nlp_analysis)
  quarantine/silver/source=S/ingestion_date=D/run_id=R/rejected.parquet (silver_quarantine)
  control/silver/batches/source=S/run_id=R/seq=N.json             (silver_batch_log)
"""
from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from . import schemas as S
from .bronze import IntegrityError, run_index_key, sanitize_run_id, sha256
from .cleaning import (build_identity, canonical_url, changed_fields, clean_html,
                       content_hash, fold, sha256_hex, validate)
from .comprehend_enricher import (ANALYZED, ANALYZED_TRUNCATED, CACHEABLE, Analysis,
                                  ComprehendEnricher, build_text, text_hash)
from .config import SilverConfig
from .lake import Lake, SimulatedFailure, iso, utcnow

log = logging.getLogger(__name__)

POINTER_KEY = "control/silver/current.json"
LOCK_NAME = "datalake-writer"
ENTITY_CATEGORY = {"PERSON": "persona", "ORGANIZATION": "organizacion", "LOCATION": "lugar"}
VOLATILE = {"silver_version", "silver_published_at_utc"}   # no forman parte de la huella


class QualityGateError(Exception):
    """El resultado no pasa las reglas de calidad: no se publica."""


class StaleWorkError(Exception):
    """El trabajo preparado se basa en un commit que ya no es el vigente."""


def to_dt(value) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def pub_date_of(row: dict) -> str:
    return to_dt(row["pub_date_utc"]).astimezone(timezone.utc).date().isoformat()


def part_key(table: str, date: str, version: str) -> str:
    return f"silver/{table}/pub_date={date}/v={version}/part-00000.parquet"


def part_prefix(table: str, date: str, version: str) -> str:
    return f"silver/{table}/pub_date={date}/v={version}/"


def _fingerprint(rows: list[dict]) -> str:
    clean = [{k: v for k, v in r.items() if k not in VOLATILE} for r in rows]
    clean.sort(key=lambda r: json.dumps(r, sort_keys=True, default=str))
    return hashlib.sha256(json.dumps(clean, sort_keys=True, default=str).encode()).hexdigest()


class SilverProcessor:
    def __init__(self, s3, bucket: str, cfg: SilverConfig,
                 enricher: ComprehendEnricher | None = None, now_fn=None):
        self.lake = Lake(s3, bucket)
        self.cfg = cfg
        self.enricher = enricher
        self.now_fn = now_fn or utcnow

    # ================================================================ estado
    def read_pointer(self) -> dict:
        return self.lake.get_json(POINTER_KEY) or {
            "sequence": 0, "run_id": None, "partitions": {}, "fingerprints": {},
            "previous_partitions": {}, "outbox": []}

    def load_news(self, pointer: dict) -> dict[str, dict]:
        state = {}
        for date, version in pointer.get("partitions", {}).items():
            raw = self.lake.get(part_key("news", date, version))
            for r in S.from_parquet_bytes(raw) if raw else []:
                state[r["news_id"]] = r
        return state

    def load_cache(self) -> dict[str, dict]:
        cache: dict[str, dict] = {}
        for k in sorted(self.lake.keys("silver/nlp_analysis/")):
            if k.endswith(".parquet"):
                for r in S.from_parquet_bytes(self.lake.get(k)):
                    cache[r["text_hash"]] = r
        return cache

    def processed_batches(self) -> set[tuple[str, str]]:
        done = set()
        for k in self.lake.keys("control/silver/batches/"):
            parts = dict(p.split("=", 1) for p in k[:-5].split("/") if "=" in p)
            done.add((parts["source"], parts["run_id"]))
        return done

    def pending_manifests(self, full_refresh: bool, force: list[str]) -> list[dict]:
        if full_refresh:
            prefixes = ["control/ingestion_log/"]
        else:
            today = self.now_fn().date()
            prefixes = [f"control/ingestion_log/ingestion_date={today - timedelta(days=d)}/"
                        for d in range(self.cfg.lookback_days + 1)]
            for rid in force:
                idx = self.lake.get_json(run_index_key(rid))
                prefixes.append(f"control/ingestion_log/ingestion_date={idx['ingestion_date']}/run_id={rid}/"
                                if idx else "control/ingestion_log/")
        processed = self.processed_batches()
        found = {}
        for prefix in dict.fromkeys(prefixes):
            for k in self.lake.keys(prefix):
                parts = dict(p.split("=", 1) for p in k[:-5].split("/") if "=" in p)
                ident = (parts.get("source"), parts.get("run_id"))
                forced = ident[1] in force
                if ident in processed and not (full_refresh or forced):
                    continue
                m = json.loads(self.lake.get(k))
                if m.get("status") == "SUCCESS" and m.get("s3_uri"):
                    m["_forced"] = forced
                    m["_already_processed"] = ident in processed
                    found[ident] = m
        return sorted(found.values(), key=lambda m: (m["ingestion_ts_utc"], m["source_id"]))

    # ================================================================ 1) prepare
    def prepare(self, run_id: str, *, full_refresh: bool = False,
                force_run_ids: list[str] | None = None) -> dict:
        run_id = sanitize_run_id(run_id)
        force = [sanitize_run_id(r) for r in (force_run_ids or []) if r]
        work = f"work/silver/run_id={run_id}/"
        pointer = self.read_pointer()
        if pointer.get("run_id") == run_id:
            return {"run_id": run_id, "already_published": True, **pointer.get("stats", {})}
        self.lake.finalize_outbox(pointer.get("outbox"))      # recupera un commit a medias

        meta = self.lake.get_json(work + "meta.json")
        if meta and meta["base_sequence"] == pointer["sequence"] and meta["full_refresh"] == full_refresh \
                and meta["force_run_ids"] == force:
            log.info("[run_id=%s] prepare ya realizado; se reutiliza", run_id)
            return meta["stats"]

        now = self.now_fn()
        state = {} if full_refresh else self.load_news(pointer)
        manifests = self.pending_manifests(full_refresh, force)
        log.info("[run_id=%s] prepare: %d noticias vigentes, %d lotes Bronze a procesar",
                 run_id, len(state), len(manifests))

        history, batch_logs, outbox, totals = [], [], [], Counter()
        for m in manifests:
            stats, rejected = self._process_batch(m, state, history, run_id, now)
            totals.update(stats)
            q_uri = None
            if rejected:
                src = f"{work}quarantine/{m['source_id']}__{m['run_id']}.parquet"
                dst = (f"quarantine/silver/source={m['source_id']}/ingestion_date={m['ingestion_date']}"
                       f"/run_id={m['run_id']}/rejected.parquet")
                self.lake.put(src, S.to_parquet_bytes(rejected, S.QUARANTINE))
                outbox.append({"src": src, "dst": dst})
                q_uri = self.lake.uri(dst)
            log.info("[run_id=%s source=%s lote=%s] %s", run_id, m["source_id"], m["run_id"], stats)
            batch_logs.append({
                "source_id": m["source_id"], "medio": m["medio"], "seccion": m["seccion"],
                "bronze_run_id": m["run_id"], "ingestion_date": m["ingestion_date"],
                "bronze_uri": m["s3_uri"], "bronze_sha256": m["content_sha256"],
                "bronze_mode": m.get("mode", "extract"), "silver_run_id": run_id,
                "processed_at_utc": iso(now), "reprocessed": bool(m["_already_processed"]),
                "full_refresh": full_refresh, "quarantine_uri": q_uri,
                **{k: stats.get(k, 0) for k in (
                    "items_read", "valid", "invalid", "duplicates_in_batch", "new", "updated",
                    "unchanged", "stale", "cross_feed_matches")}})

        if history and not full_refresh:
            self.lake.put(work + "history.parquet", S.to_parquet_bytes(history, S.NEWS_HISTORY))
        self.lake.put(work + "state.parquet", S.to_parquet_bytes(list(state.values()), S.NEWS))
        stats = {"run_id": run_id, "base_sequence": pointer["sequence"], "batches": len(manifests),
                 "news_in_state": len(state), "full_refresh": full_refresh,
                 "history_rows": 0 if full_refresh else len(history),
                 **{k: totals.get(k, 0) for k in ("items_read", "valid", "invalid",
                    "duplicates_in_batch", "new", "updated", "unchanged", "stale", "cross_feed_matches")}}
        self.lake.put_json(work + "meta.json", {
            "base_sequence": pointer["sequence"], "full_refresh": full_refresh,
            "force_run_ids": force, "batch_logs": batch_logs, "outbox": outbox,
            "has_history": bool(history) and not full_refresh, "stats": stats,
            "prepared_at_utc": iso(now)})
        return stats

    def _process_batch(self, m: dict, state: dict, history: list, run_id: str, now) -> tuple[dict, list]:
        content = self.lake.get(self.lake.key_of(m["s3_uri"]))
        if content is None or sha256(content) != m["content_sha256"]:
            raise IntegrityError(f"Bronze ausente o alterado: {m['s3_uri']}")
        from .rss_parser import parse_feed
        items = parse_feed(content, source_id=m["source_id"], medio=m["medio"], seccion=m["seccion"],
                           source_url=m["source_url"], extracted_at_utc=m["ingestion_ts_utc"],
                           run_id=m["run_id"])
        ingested = to_dt(m["ingestion_ts_utc"])
        stats = Counter(items_read=len(items))
        rejected, seen = [], set()
        for idx, it in enumerate(items):
            title, desc = clean_html(it.title), clean_html(it.description)
            link = it.link.strip() if it.link else None
            reasons = validate(title=title, link=link, pub_date_raw=it.pub_date_raw,
                               pub_date_utc=it.pub_date_utc, extracted_at_utc=m["ingestion_ts_utc"],
                               future_tolerance_hours=self.cfg.future_tolerance_hours)
            if reasons:
                stats["invalid"] += 1
                rejected.append({
                    "source_id": m["source_id"], "medio": m["medio"], "seccion": m["seccion"],
                    "bronze_run_id": m["run_id"], "ingestion_date": m["ingestion_date"],
                    "bronze_uri": m["s3_uri"], "item_index": idx, "item_id_raw": it.item_id,
                    "title_raw": it.title, "link_raw": it.link, "description_raw": it.description,
                    "pub_date_raw": it.pub_date_raw, "reasons": reasons, "reason": ";".join(reasons),
                    "quarantined_at_utc": now, "silver_run_id": run_id})
                continue
            news_id, strategy, key = build_identity(guid=it.guid, link=link, title=title,
                                                    pub_date_utc=it.pub_date_utc, medio=m["medio"])
            if news_id in seen:
                stats["duplicates_in_batch"] += 1
                continue
            seen.add(news_id)
            stats["valid"] += 1
            link_c = canonical_url(link)
            rec = {"news_id": news_id, "id_strategy": strategy, "identity_key": key, "guid": it.guid,
                   "link": link, "link_canonical": link_c, "title": title, "title_raw": it.title,
                   "description": desc, "description_raw": it.description,
                   "pub_date_utc": to_dt(it.pub_date_utc), "pub_date_raw": it.pub_date_raw,
                   "title_fingerprint": sha256_hex(fold(title))[:16],
                   "content_hash": content_hash(title, desc, link_c, it.pub_date_utc)}
            result, cross = self._merge(state, rec, m, ingested, history, run_id, now)
            stats[result] += 1
            stats["cross_feed_matches"] += int(cross)
        return dict(stats), rejected

    def _merge(self, state, rec, m, ingested, history, run_id, now) -> tuple[str, bool]:
        nid, src, sec = rec["news_id"], m["source_id"], m["seccion"]
        ex = state.get(nid)
        if ex is None:
            state[nid] = {**rec, "medio": m["medio"], "first_source_id": src, "first_seccion": sec,
                          "source_ids": [src], "secciones": [sec], "n_sources": 1, "version": 1,
                          "first_seen_at_utc": ingested, "last_change_at_utc": ingested,
                          "first_bronze_run_id": m["run_id"], "first_bronze_uri": m["s3_uri"],
                          "bronze_run_id": m["run_id"], "bronze_uri": m["s3_uri"],
                          "bronze_sha256": m["content_sha256"]}
            history.append(_history_row(state[nid], "NEW", [], None, True, src, run_id, now))
            return "new", False

        cross = src not in (ex["source_ids"] or [])
        if cross:
            ex["source_ids"] = list(ex["source_ids"]) + [src]
            ex["secciones"] = list(ex["secciones"]) + [sec]
            ex["n_sources"] = len(ex["source_ids"])
        if rec["content_hash"] == ex["content_hash"]:
            return "unchanged", cross
        if ingested < to_dt(ex["last_change_at_utc"]):
            return "stale", cross        # un lote más viejo no pisa la versión vigente
        fields = changed_fields(ex, rec)
        prev_hash = ex["content_hash"]
        text_changed = build_text(ex["title"], ex["description"]) != build_text(rec["title"], rec["description"])
        for f in ("guid", "link", "link_canonical", "title", "title_raw", "description",
                  "description_raw", "pub_date_utc", "pub_date_raw", "title_fingerprint", "content_hash"):
            ex[f] = rec[f]
        ex["version"] += 1
        ex["last_change_at_utc"] = ingested
        ex["bronze_run_id"], ex["bronze_uri"], ex["bronze_sha256"] = m["run_id"], m["s3_uri"], m["content_sha256"]
        history.append(_history_row(ex, "UPDATED", fields, prev_hash, text_changed, src, run_id, now))
        return "updated", cross

    # ================================================================ 2) enrich
    def enrich(self, run_id: str) -> dict:
        run_id = sanitize_run_id(run_id)
        work = f"work/silver/run_id={run_id}/"
        if self.read_pointer().get("run_id") == run_id:
            return {"run_id": run_id, "already_published": True}
        raw = self.lake.get(work + "state.parquet")
        if raw is None:
            raise StaleWorkError(f"No existe {work}state.parquet: ejecute prepare primero")
        state = S.from_parquet_bytes(raw)
        cache = self.load_cache()
        texts = {}
        for row in state:
            text = build_text(row["title"], row["description"])
            h = text_hash(text)
            if h not in cache:
                texts[h] = text
        stats = {"run_id": run_id, "texts_total": len(state), "texts_cached": len(state) - len(texts),
                 "texts_to_analyze": len(texts), "analyzed_now": 0, "statuses": {}}
        if texts and self.enricher and self.enricher.cfg.enabled:
            results = self.enricher.analyze(texts)
            rows = [_analysis_row(a) for a in results.values() if a.status in CACHEABLE]
            stats["statuses"] = dict(Counter(a.status for a in results.values()))
            stats["comprehend_calls"] = dict(self.enricher.calls)
            if rows:
                digest = hashlib.sha256("".join(sorted(r["text_hash"] for r in rows)).encode()).hexdigest()[:12]
                day = self.now_fn().date().isoformat()
                self.lake.put(f"silver/nlp_analysis/analyzed_date={day}/part-{run_id}-{digest}.parquet",
                              S.to_parquet_bytes(rows, S.NLP_ANALYSIS))
                stats["analyzed_now"] = len(rows)
            self.lake.put_json(work + "enrich_failures.json", {
                h: {"status": a.status, "error": a.error} for h, a in results.items()
                if a.status not in CACHEABLE})
        log.info("[run_id=%s] enrich: %s", run_id, stats)
        return stats

    # ================================================================ 3) publish
    def publish(self, run_id: str, *, simulate_failure: bool = False) -> dict:
        run_id = sanitize_run_id(run_id)
        work = f"work/silver/run_id={run_id}/"
        pointer = self.read_pointer()
        if pointer.get("run_id") == run_id:                      # reintento tras commit
            self.lake.finalize_outbox(pointer.get("outbox"))
            return {**pointer.get("stats", {}), "committed": True, "already_published": True}
        meta = self.lake.get_json(work + "meta.json")
        if meta is None:
            raise StaleWorkError(f"No existe {work}meta.json: ejecute prepare primero")
        if meta["base_sequence"] != pointer["sequence"]:
            raise StaleWorkError(f"prepare se basó en la secuencia {meta['base_sequence']} pero la vigente es "
                                 f"{pointer['sequence']}; repita desde prepare")

        now = self.now_fn()
        state = S.from_parquet_bytes(self.lake.get(work + "state.parquet"))
        cache = self.load_cache()
        failures = self.lake.get_json(work + "enrich_failures.json", {}) or {}
        for row in state:
            self._apply_nlp(row, cache, failures)
        by_date: dict[str, dict[str, list]] = defaultdict(lambda: {"news": [], "news_entities": [],
                                                                    "news_key_phrases": []})
        for row in state:
            d = pub_date_of(row)
            ents, kps = self._entities_and_phrases(row, cache)
            by_date[d]["news"].append(row)
            by_date[d]["news_entities"] += ents
            by_date[d]["news_key_phrases"] += kps

        self._quality_gate(state, by_date)
        fps = {d: _fingerprint(t["news"] + t["news_entities"] + t["news_key_phrases"])
               for d, t in by_date.items()}
        old_parts, old_fps = pointer.get("partitions", {}), pointer.get("fingerprints", {})
        dirty = sorted(d for d in fps if fps[d] != old_fps.get(d) or d not in old_parts)
        removed = sorted(d for d in old_parts if d not in fps)
        new_parts = {d: (run_id if d in dirty else old_parts[d]) for d in fps}

        batch_logs = meta["batch_logs"]
        if not dirty and not removed and not batch_logs and pointer["sequence"] > 0:
            log.info("[run_id=%s] sin cambios en Silver: no hay commit", run_id)
            return {"run_id": run_id, "committed": False, "dirty_dates": [], "batches": 0,
                    "news_current": len(state)}

        self.lake.acquire_lock(LOCK_NAME, run_id)
        try:
            for d in dirty:                                   # 1) datos versionados (invisibles aún)
                for table, schema in S.SILVER_TABLES.items():
                    rows = by_date[d][table]
                    for r in rows:
                        r["silver_version"], r["silver_published_at_utc"] = run_id, now
                    self.lake.put(part_key(table, d, run_id), S.to_parquet_bytes(rows, schema))

            seq = pointer["sequence"] + 1                      # 2) outbox
            outbox = list(meta["outbox"])
            if meta["has_history"]:
                outbox.append({"src": work + "history.parquet",
                               "dst": f"silver/news_history/processed_date={now.date()}/part-{run_id}.parquet"})
            for b in batch_logs:
                src = f"{work}batches/{b['source_id']}__{b['bronze_run_id']}.json"
                self.lake.put_json(src, {**b, "sequence": seq})
                outbox.append({"src": src, "dst": f"control/silver/batches/source={b['source_id']}"
                                                  f"/run_id={b['bronze_run_id']}/seq={seq}.json"})

            if simulate_failure:
                raise SimulatedFailure("Fallo simulado en silver_publish antes del commit")

            stats = {**meta["stats"], "sequence": seq, "dirty_dates": dirty, "removed_dates": removed,
                     "partitions_total": len(new_parts), "news_current": len(state),
                     "entities_current": sum(len(t["news_entities"]) for t in by_date.values()),
                     "key_phrases_current": sum(len(t["news_key_phrases"]) for t in by_date.values()),
                     "nlp_status": dict(Counter(r["nlp_status"] for r in state))}
            new_pointer = {"sequence": seq, "run_id": run_id, "committed_at_utc": iso(now),
                           "partitions": new_parts, "fingerprints": fps,
                           "previous_partitions": old_parts, "outbox": outbox, "stats": stats}
            self.lake.put_json(POINTER_KEY, new_pointer)       # 3) COMMIT
            self.lake.finalize_outbox(outbox)                  # 4) post-commit
            self.lake.put_json(f"control/silver/runs/silver_run_id={run_id}.json", new_pointer)
        finally:
            self.lake.release_lock(LOCK_NAME, run_id)
        log.info("[run_id=%s] Silver publicado: seq=%s, particiones reescritas=%s", run_id, seq, dirty)
        return {**stats, "committed": True}

    def run(self, run_id: str, *, full_refresh=False, force_run_ids=None) -> dict:
        self.prepare(run_id, full_refresh=full_refresh, force_run_ids=force_run_ids)
        enr = self.enrich(run_id)
        out = self.publish(run_id)
        return {**out, "enrich": enr}

    # ================================================================ helpers
    def _apply_nlp(self, row: dict, cache: dict, failures: dict | None = None) -> None:
        row["text_hash"] = text_hash(build_text(row["title"], row["description"]))
        a = cache.get(row["text_hash"])
        if a:
            row["nlp_status"], row["nlp_language"] = a["status"], a["language"]
            row["nlp_analyzed_at_utc"] = a["analyzed_at_utc"]
        else:
            # ERROR / PENDING_QUOTA no se guardan en caché: se reintentan en la próxima ejecución
            fail = (failures or {}).get(row["text_hash"])
            enabled = bool(self.enricher and self.enricher.cfg.enabled)
            row["nlp_status"] = fail["status"] if fail else ("PENDING" if enabled else "NOT_ANALYZED")
            row["nlp_language"], row["nlp_analyzed_at_utc"] = None, None

    def _entities_and_phrases(self, row: dict, cache: dict) -> tuple[list, list]:
        a = cache.get(row["text_hash"])
        if not a or a["status"] not in (ANALYZED, ANALYZED_TRUNCATED):
            return [], []
        base = {"news_id": row["news_id"], "text_hash": row["text_hash"], "language": a["language"],
                "analyzed_at_utc": a["analyzed_at_utc"], "pub_date_utc": row["pub_date_utc"],
                "medio": row["medio"], "first_source_id": row["first_source_id"],
                "secciones": row["secciones"]}
        ents, kps = [], []
        groups = defaultdict(list)
        for e in a["entities"] or []:
            if e["type"] in self.cfg.entity_types and fold(e["text"]):
                groups[(e["type"], fold(e["text"]))].append(e)
        for (etype, norm), es in sorted(groups.items()):
            sc = [e["score"] for e in es]
            ents.append({**base, "entity_type": etype, "entity_category": ENTITY_CATEGORY.get(etype, etype.lower()),
                         "entity_text": Counter(e["text"] for e in es).most_common(1)[0][0],
                         "entity_text_norm": norm, "mention_count": len(es),
                         "max_score": max(sc), "avg_score": round(sum(sc) / len(sc), 4)})
        groups = defaultdict(list)
        for k in a["key_phrases"] or []:
            if fold(k["text"]):
                groups[fold(k["text"])].append(k)
        for norm, ks in sorted(groups.items()):
            sc = [k["score"] for k in ks]
            kps.append({**base, "key_phrase": Counter(k["text"] for k in ks).most_common(1)[0][0],
                        "key_phrase_norm": norm, "mention_count": len(ks),
                        "max_score": max(sc), "avg_score": round(sum(sc) / len(sc), 4)})
        return ents, kps

    def _quality_gate(self, state: list[dict], by_date: dict) -> None:
        errors = []
        ids = [r["news_id"] for r in state]
        if len(ids) != len(set(ids)):
            errors.append(f"news_id duplicado: {len(ids) - len(set(ids))}")
        for r in state:
            if not r.get("title") or not r.get("link") or r.get("pub_date_utc") is None:
                errors.append(f"campos obligatorios vacíos en {r['news_id']}")
        news_ids = set(ids)
        for t in by_date.values():
            for e in t["news_entities"] + t["news_key_phrases"]:
                if e["news_id"] not in news_ids:
                    errors.append(f"entidad/frase huérfana {e['news_id']}")
            keys = [(e["news_id"], e["entity_type"], e["entity_text_norm"]) for e in t["news_entities"]]
            if len(keys) != len(set(keys)):
                errors.append("entidad repetida dentro de una noticia")
        if errors:
            raise QualityGateError("; ".join(errors[:10]))


def _history_row(row, change_type, fields, prev_hash, text_changed, src, run_id, now) -> dict:
    return {"news_id": row["news_id"], "version": row["version"], "change_type": change_type,
            "changed_fields": fields, "text_changed": text_changed, "title": row["title"],
            "description": row["description"], "link": row["link"], "pub_date_utc": row["pub_date_utc"],
            "content_hash": row["content_hash"], "previous_content_hash": prev_hash, "source_id": src,
            "bronze_run_id": row["bronze_run_id"], "bronze_uri": row["bronze_uri"],
            "processed_at_utc": now, "silver_run_id": run_id}


def _analysis_row(a: Analysis) -> dict:
    return {"text_hash": a.text_hash, "analyzed_text": a.analyzed_text, "text_bytes": a.text_bytes,
            "truncated": a.truncated, "status": a.status, "language": a.language,
            "language_score": a.language_score, "language_source": a.language_source,
            "analyzed_at_utc": to_dt(a.analyzed_at_utc), "analysis_version": a.analysis_version,
            "entities": a.entities, "key_phrases": a.key_phrases}
