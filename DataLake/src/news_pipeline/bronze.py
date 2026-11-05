"""Ingesta y capa Bronze (punto 4.2).

Organización en el bucket único del data lake:

  bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/feed.xml
      Contenido original de las respuestas válidas (byte a byte).
  quarantine/bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/response.bin
      Respuestas 200 vacías o con XML malformado (para diagnóstico, no es Bronze).
  staging/parsed/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/items.jsonl
      Campos extraídos del RSS. Se regenera al reprocesar (misma clave = sin duplicados).
  control/ingestion_log/ingestion_date=AAAA-MM-DD/run_id=<id>/source=<fuente>.json
      Manifiesto del lote: run_id, fuente, URL, ts UTC, HTTP, hash, ubicación S3...
  control/reprocess_log/reprocess_date=AAAA-MM-DD/reprocess_id=<id>/source=<fuente>.json
      Auditoría de reprocesamientos.
  control/latest/source=<fuente>.json
      Último hash exitoso por fuente (detecta feeds sin cambios).

Garantías:
  * Idempotencia: las claves dependen solo de (fuente, fecha, run_id). Reintentar
    una tarea sobrescribe los mismos objetos; si el manifiesto ya está en SUCCESS
    no se vuelve a consultar la fuente.
  * El manifiesto se escribe DESPUÉS del contenido: actúa como marca de commit.
  * Reprocesar lee Bronze, verifica el hash y regenera staging. Nunca escribe en
    bronze/ ni consulta la fuente, por lo que no crea otra copia lógica del lote.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from .config import HttpConfig, Source
from .http_client import FetchResult, fetch
from .rss_parser import EmptyFeedError, MalformedFeedError, NewsItem, parse_feed

log = logging.getLogger(__name__)

# Estados de un lote
SUCCESS = "SUCCESS"
EMPTY = "EMPTY_RESPONSE"
HTTP_ERROR = "HTTP_ERROR"
NETWORK_ERROR = "NETWORK_ERROR"
MALFORMED = "MALFORMED_XML"


class BatchNotFoundError(Exception):
    pass


class IntegrityError(Exception):
    pass


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sanitize_run_id(run_id: str) -> str:
    """'scheduled__2026-10-22T11:00:00+00:00' -> 'scheduled__2026-10-22T11-00-00-00-00'.
    Evita ':' '+' '/' y '=' que rompen las particiones estilo Hive."""
    clean = re.sub(r"[^A-Za-z0-9_\-]", "-", run_id).strip("-")
    if not clean:
        raise ValueError("run_id vacío")
    return clean[:200]


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------- claves S3
def bronze_key(source_id: str, ingestion_date: str, run_id: str) -> str:
    return f"bronze/source={source_id}/ingestion_date={ingestion_date}/run_id={run_id}/feed.xml"


def quarantine_key(source_id: str, ingestion_date: str, run_id: str) -> str:
    return f"quarantine/bronze/source={source_id}/ingestion_date={ingestion_date}/run_id={run_id}/response.bin"


def staging_key(source_id: str, ingestion_date: str, run_id: str) -> str:
    return f"staging/parsed/source={source_id}/ingestion_date={ingestion_date}/run_id={run_id}/items.jsonl"


def manifest_key(source_id: str, ingestion_date: str, run_id: str) -> str:
    return f"control/ingestion_log/ingestion_date={ingestion_date}/run_id={run_id}/source={source_id}.json"


def latest_key(source_id: str) -> str:
    return f"control/latest/source={source_id}.json"


def run_index_key(run_id: str) -> str:
    return f"control/run_index/run_id={run_id}.json"


# --------------------------------------------------------------------------- manifiesto
@dataclass
class BatchManifest:
    run_id: str
    source_id: str
    medio: str
    seccion: str
    source_url: str
    ingestion_ts_utc: str
    ingestion_date: str
    status: str
    http_status: int | None
    content_sha256: str | None
    content_bytes: int
    item_count: int
    s3_uri: str | None                 # ubicación del archivo original en Bronze
    staging_uri: str | None
    quarantine_uri: str | None
    content_unchanged: bool            # mismo hash que el último lote exitoso de la fuente
    previous_run_id: str | None
    attempts: int
    elapsed_ms: int
    error: str | None
    response_headers: dict = field(default_factory=dict)
    mode: str = "extract"              # extract | controlled_copy
    derived_from_run_id: str | None = None
    reprocess_count: int = 0
    last_reprocessed_at_utc: str | None = None
    schema_version: int = 1

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)


class BronzeStore:
    def __init__(self, s3_client, bucket: str):
        self.s3 = s3_client
        self.bucket = bucket

    # -- utilidades S3 ------------------------------------------------------
    def _uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def _get(self, key: str) -> bytes | None:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    def _put(self, key: str, body: bytes, content_type: str, metadata: dict | None = None) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body,
                           ContentType=content_type, Metadata=metadata or {})

    def get_manifest(self, source_id: str, ingestion_date: str, run_id: str) -> BatchManifest | None:
        raw = self._get(manifest_key(source_id, ingestion_date, run_id))
        return BatchManifest(**json.loads(raw)) if raw else None

    def resolve_ingestion_date(self, run_id: str, default: str) -> str:
        """Fija la fecha de ingesta de un run_id la primera vez que se usa.
        Así, aunque la tarea se reintente pasada la medianoche UTC, el lote
        sigue escribiéndose en la misma partición (no se crea otra copia)."""
        raw = self._get(run_index_key(run_id))
        if raw:
            return json.loads(raw)["ingestion_date"]
        self._put(run_index_key(run_id), json.dumps(
            {"run_id": run_id, "ingestion_date": default, "created_at_utc": iso(utcnow())}
        ).encode(), "application/json")
        return default

    def find_manifest(self, source_id: str, run_id: str) -> BatchManifest:
        """Localiza un lote solo por (fuente, run_id), sin conocer la fecha."""
        idx = self._get(run_index_key(run_id))
        if idx:
            m = self.get_manifest(source_id, json.loads(idx)["ingestion_date"], run_id)
            if m:
                return m
        paginator = self.s3.get_paginator("list_objects_v2")  # respaldo: búsqueda completa
        suffix = f"/run_id={run_id}/source={source_id}.json"
        for page in paginator.paginate(Bucket=self.bucket, Prefix="control/ingestion_log/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(suffix):
                    return BatchManifest(**json.loads(self._get(obj["Key"])))
        raise BatchNotFoundError(f"No existe el lote run_id={run_id} source={source_id}")

    def _write_staging(self, items: list[NewsItem], key: str) -> str:
        body = "\n".join(json.dumps(i.to_dict(), ensure_ascii=False) for i in items) + "\n"
        self._put(key, body.encode("utf-8"), "application/x-ndjson")
        return self._uri(key)

    # -- nueva extracción ---------------------------------------------------
    def ingest(self, source: Source, run_id: str, http: HttpConfig,
               ingestion_date: str | None = None, fetcher=fetch) -> BatchManifest:
        run_id = sanitize_run_id(run_id)
        now = utcnow()
        ingestion_date = ingestion_date or self.resolve_ingestion_date(run_id, now.date().isoformat())
        mkey = manifest_key(source.source_id, ingestion_date, run_id)

        existing = self.get_manifest(source.source_id, ingestion_date, run_id)
        if existing and existing.status == SUCCESS:
            log.info("Lote %s/%s ya ingerido; no se vuelve a consultar la fuente",
                     source.source_id, run_id)
            return existing

        res: FetchResult = fetcher(source.url, timeout=http.timeout_seconds,
                                   max_retries=http.max_retries,
                                   backoff_factor=http.backoff_factor,
                                   user_agent=http.user_agent)
        content_hash = sha256(res.content) if res.content else None
        prev = self._get(latest_key(source.source_id))
        prev = json.loads(prev) if prev else None

        manifest = BatchManifest(
            run_id=run_id, source_id=source.source_id, medio=source.medio,
            seccion=source.seccion, source_url=source.url, ingestion_ts_utc=iso(now),
            ingestion_date=ingestion_date, status="", http_status=res.http_status,
            content_sha256=content_hash, content_bytes=len(res.content), item_count=0,
            s3_uri=None, staging_uri=None, quarantine_uri=None,
            content_unchanged=bool(prev and content_hash and prev["content_sha256"] == content_hash),
            previous_run_id=prev["run_id"] if prev else None,
            attempts=res.attempts, elapsed_ms=res.elapsed_ms, error=res.error,
            response_headers=res.headers,
        )

        if res.http_status is None:
            manifest.status = NETWORK_ERROR
        elif res.http_status != 200:
            manifest.status = HTTP_ERROR
        else:
            try:
                items = parse_feed(res.content, source_id=source.source_id,
                                   medio=source.medio, seccion=source.seccion,
                                   source_url=source.url, extracted_at_utc=iso(now),
                                   run_id=run_id)
            except EmptyFeedError as exc:
                manifest.status, manifest.error = EMPTY, str(exc)
            except MalformedFeedError as exc:
                manifest.status, manifest.error = MALFORMED, str(exc)
            else:
                # 1) contenido original  2) staging  3) manifiesto (commit)
                bkey = bronze_key(source.source_id, ingestion_date, run_id)
                self._put(bkey, res.content, res.content_type or "application/xml",
                          {"sha256": content_hash, "run_id": run_id,
                           "source_id": source.source_id, "source_url": source.url})
                manifest.s3_uri = self._uri(bkey)
                manifest.staging_uri = self._write_staging(
                    items, staging_key(source.source_id, ingestion_date, run_id))
                manifest.item_count = len(items)
                manifest.status = SUCCESS

        if manifest.status in (EMPTY, MALFORMED) and res.content:
            qkey = quarantine_key(source.source_id, ingestion_date, run_id)
            self._put(qkey, res.content, "application/octet-stream")
            manifest.quarantine_uri = self._uri(qkey)

        self._put(mkey, manifest.to_json().encode("utf-8"), "application/json")
        if manifest.status == SUCCESS:
            self._put(latest_key(source.source_id), json.dumps(
                {"run_id": run_id, "content_sha256": content_hash,
                 "ingestion_ts_utc": manifest.ingestion_ts_utc}).encode(), "application/json")
        log.info("Lote %s/%s -> %s (HTTP %s, %d ítems)", source.source_id, run_id,
                 manifest.status, manifest.http_status, manifest.item_count)
        return manifest

    # -- copia controlada (demostraciones) -----------------------------------
    def store_controlled(self, source: Source, run_id: str, content: bytes,
                         derived_from_run_id: str, note: str) -> BatchManifest:
        """Guarda como lote Bronze NUEVO una copia modificada de un RSS ya almacenado.

        Sirve para demostrar noticias modificadas o registros defectuosos sin depender
        de que el medio publique algo. Queda marcado con mode=controlled_copy y la
        referencia al lote original, así no se confunde con una extracción real.
        """
        run_id = sanitize_run_id(run_id)
        now = utcnow()
        ingestion_date = self.resolve_ingestion_date(run_id, now.date().isoformat())
        items = parse_feed(content, source_id=source.source_id, medio=source.medio,
                           seccion=source.seccion, source_url=source.url,
                           extracted_at_utc=iso(now), run_id=run_id)
        bkey = bronze_key(source.source_id, ingestion_date, run_id)
        digest = sha256(content)
        self._put(bkey, content, "application/xml",
                  {"sha256": digest, "run_id": run_id, "source_id": source.source_id,
                   "derived_from_run_id": derived_from_run_id})
        m = BatchManifest(
            run_id=run_id, source_id=source.source_id, medio=source.medio, seccion=source.seccion,
            source_url=source.url, ingestion_ts_utc=iso(now), ingestion_date=ingestion_date,
            status=SUCCESS, http_status=None, content_sha256=digest, content_bytes=len(content),
            item_count=len(items), s3_uri=self._uri(bkey),
            staging_uri=self._write_staging(items, staging_key(source.source_id, ingestion_date, run_id)),
            quarantine_uri=None, content_unchanged=False, previous_run_id=derived_from_run_id,
            attempts=0, elapsed_ms=0, error=None, response_headers={"note": note},
            mode="controlled_copy", derived_from_run_id=derived_from_run_id)
        self._put(manifest_key(source.source_id, ingestion_date, run_id),
                  m.to_json().encode("utf-8"), "application/json")
        return m

    def latest_success(self, source_id: str) -> BatchManifest:
        """Último lote SUCCESS de una fuente que vino de una extracción real."""
        best = None
        for k in self._keys("control/ingestion_log/"):
            if k.endswith(f"/source={source_id}.json"):
                m = BatchManifest(**json.loads(self._get(k)))
                if m.status == SUCCESS and m.mode == "extract" and (best is None or m.ingestion_ts_utc > best.ingestion_ts_utc):
                    best = m
        if best is None:
            raise BatchNotFoundError(f"No hay lotes exitosos de {source_id}")
        return best

    def _keys(self, prefix: str) -> list[str]:
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            out += [o["Key"] for o in page.get("Contents", [])]
        return out

    def read_bronze(self, m: BatchManifest) -> bytes:
        content = self._get(m.s3_uri.split(f"s3://{self.bucket}/", 1)[1])
        if content is None or sha256(content) != m.content_sha256:
            raise IntegrityError(f"Bronze ausente o alterado: {m.s3_uri}")
        return content

    # -- reprocesamiento ----------------------------------------------------
    def reprocess(self, source_id: str, run_id: str,
                  reprocess_id: str | None = None) -> tuple[BatchManifest, list[NewsItem]]:
        """Re-lee el XML original desde Bronze (sin HTTP), verifica el hash y
        regenera staging en la MISMA clave. No crea un lote nuevo."""
        run_id = sanitize_run_id(run_id)
        m = self.find_manifest(source_id, run_id)
        if m.status != SUCCESS or not m.s3_uri:
            raise BatchNotFoundError(f"El lote {source_id}/{run_id} no tiene contenido en Bronze ({m.status})")

        bkey = bronze_key(source_id, m.ingestion_date, run_id)
        content = self._get(bkey)
        if content is None:
            raise BatchNotFoundError(f"Falta el objeto Bronze {self._uri(bkey)}")
        if sha256(content) != m.content_sha256:
            raise IntegrityError(f"Hash distinto para {self._uri(bkey)}")

        # extracted_at se conserva: es el momento real de la extracción original
        items = parse_feed(content, source_id=m.source_id, medio=m.medio, seccion=m.seccion,
                           source_url=m.source_url, extracted_at_utc=m.ingestion_ts_utc,
                           run_id=run_id)
        self._write_staging(items, staging_key(source_id, m.ingestion_date, run_id))

        now = utcnow()
        m.reprocess_count += 1
        m.last_reprocessed_at_utc = iso(now)
        m.item_count = len(items)
        self._put(manifest_key(source_id, m.ingestion_date, run_id),
                  m.to_json().encode("utf-8"), "application/json")

        reprocess_id = sanitize_run_id(reprocess_id or f"reprocess-{uuid.uuid4().hex[:12]}")
        audit = {"reprocess_id": reprocess_id, "run_id": run_id, "source_id": source_id,
                 "reprocessed_at_utc": iso(now), "bronze_uri": m.s3_uri,
                 "content_sha256": m.content_sha256, "item_count": len(items)}
        self._put(f"control/reprocess_log/reprocess_date={now.date().isoformat()}/"
                  f"reprocess_id={reprocess_id}/source={source_id}.json",
                  json.dumps(audit).encode("utf-8"), "application/json")
        return m, items
