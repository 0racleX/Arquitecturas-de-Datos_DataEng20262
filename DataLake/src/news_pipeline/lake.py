"""Utilidades de publicación sobre el bucket único.

Modelo de publicación (Silver y Gold):
  * Cada partición de fecha se escribe en un prefijo versionado inmutable:
        <capa>/<tabla>/pub_date=AAAA-MM-DD/v=<run_id>/part-00000.parquet
  * Un puntero JSON (control/<capa>/current.json) dice qué versión de cada
    partición está vigente. Reescribir el puntero es una sola escritura atómica
    en S3: es el COMMIT.
  * Glue Data Catalog se sincroniza con el puntero (UpdatePartition cambia la
    ubicación de la partición). Athena solo lee lo que Glue apunta, así que nunca
    ve archivos a medio escribir ni dos versiones de la misma partición.
  * Lo que no es particionado por versión (historial, cuarentena, marcadores de
    lote) se deja en work/ y se copia a su ruta final DESPUÉS del commit
    (patrón outbox). Si el proceso cae entre ambos pasos, la siguiente ejecución
    termina la copia.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class LockBusyError(Exception):
    """Otra ejecución está escribiendo en el lake."""


class SimulatedFailure(Exception):
    """Fallo provocado a propósito para demostrar la recuperación."""


class Lake:
    def __init__(self, s3, bucket: str):
        self.s3, self.bucket = s3, bucket

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def key_of(self, uri: str) -> str:
        return uri.split(f"s3://{self.bucket}/", 1)[1]

    def get(self, key: str) -> bytes | None:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def put(self, key: str, body: bytes, ctype: str = "application/octet-stream") -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=ctype)

    def put_json(self, key: str, obj) -> None:
        self.put(key, json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"),
                 "application/json")

    def get_json(self, key: str, default=None):
        raw = self.get(key)
        return json.loads(raw) if raw else default

    def copy(self, src: str, dst: str) -> None:
        self.s3.copy_object(Bucket=self.bucket, Key=dst,
                            CopySource={"Bucket": self.bucket, "Key": src})

    def keys(self, prefix: str) -> list[str]:
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            out += [o["Key"] for o in page.get("Contents", [])]
        return out

    def subprefixes(self, prefix: str) -> list[str]:
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
            out += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
        return out

    def delete_keys(self, keys: list[str]) -> None:
        for i in range(0, len(keys), 1000):
            self.s3.delete_objects(Bucket=self.bucket, Delete={
                "Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})

    def delete_prefix(self, prefix: str) -> None:
        self.delete_keys(self.keys(prefix))

    # ------------------------------------------------------------------ outbox
    def finalize_outbox(self, outbox: list[dict]) -> int:
        """Copia work/ -> ruta final para cada entrada que aún no exista. Idempotente."""
        done = 0
        for item in outbox or []:
            if not self.exists(item["dst"]):
                self.copy(item["src"], item["dst"])
                done += 1
        return done

    # ------------------------------------------------------------------ lock
    def acquire_lock(self, name: str, owner: str, ttl_minutes: int = 30) -> None:
        """Lock con escritura condicional de S3 (If-None-Match). Re-entrante para el mismo owner."""
        key = f"control/locks/{name}.lock"
        body = json.dumps({"owner": owner, "expires_at": iso(utcnow() + timedelta(minutes=ttl_minutes))})
        for _ in range(2):
            try:
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode(), IfNoneMatch="*")
                return
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("PreconditionFailed", "ConditionalRequestConflict"):
                    raise
            current = self.get_json(key, {}) or {}
            if current.get("owner") == owner:
                return
            expires = current.get("expires_at")
            if expires and datetime.fromisoformat(expires.replace("Z", "+00:00")) < utcnow():
                log.warning("Lock %s vencido (dueño %s); se libera", name, current.get("owner"))
                self.s3.delete_object(Bucket=self.bucket, Key=key)
                continue
            raise LockBusyError(f"Lock {name} ocupado por {current.get('owner')} hasta {expires}")
        raise LockBusyError(f"No se pudo tomar el lock {name}")

    def release_lock(self, name: str, owner: str) -> None:
        key = f"control/locks/{name}.lock"
        current = self.get_json(key, {}) or {}
        if current.get("owner") == owner:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
