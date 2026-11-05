"""Actualización del AWS Glue Data Catalog (paso 7 del DAG).

Las TABLAS las define Terraform (columnas, tipos, formato Parquet, clave de
partición pub_date). Las PARTICIONES las registra este módulo, porque cambian en
cada ejecución:

  * Partición nueva (una fecha que aparece por primera vez) -> BatchCreatePartition
  * Partición reescrita (nueva versión v=<run>)               -> BatchUpdatePartition
  * Partición que se quedó sin noticias                       -> BatchDeletePartition

Cada partición apunta a la versión vigente según los punteros de Silver y Gold.
Es idempotente: si el paso falla, la siguiente ejecución deja el catálogo igual
a los punteros. Después se borran las versiones viejas que ya nadie referencia
(se conserva también la anterior).

Se eligieron definiciones explícitas en vez de un Glue Crawler: el esquema lo
controla el código (no se infiere), no hay que pagar ni esperar al crawler y la
partición queda visible en Athena en el mismo DAG, justo después de publicarse.
"""
from __future__ import annotations

import logging

from .gold import GOLD_POINTER
from .lake import Lake
from .silver import POINTER_KEY as SILVER_POINTER

log = logging.getLogger(__name__)

TABLES = {
    # tabla Glue: (puntero, prefijo S3)
    "silver_news": (SILVER_POINTER, "silver/news"),
    "silver_news_entities": (SILVER_POINTER, "silver/news_entities"),
    "silver_news_key_phrases": (SILVER_POINTER, "silver/news_key_phrases"),
    "gold_news_daily": (GOLD_POINTER, "gold/news_daily"),
    "gold_entity_daily": (GOLD_POINTER, "gold/entity_daily"),
    "gold_key_phrase_section_daily": (GOLD_POINTER, "gold/key_phrase_section_daily"),
}


class CatalogSync:
    def __init__(self, glue, s3, bucket: str, database: str):
        self.glue, self.lake, self.db = glue, Lake(s3, bucket), database

    def _existing(self, table: str) -> dict[str, str]:
        out = {}
        for page in self.glue.get_paginator("get_partitions").paginate(DatabaseName=self.db, TableName=table):
            for p in page["Partitions"]:
                out[p["Values"][0]] = p["StorageDescriptor"]["Location"]
        return out

    def _partition_input(self, sd: dict, date: str, location: str) -> dict:
        keep = ("Columns", "InputFormat", "OutputFormat", "SerdeInfo", "Compressed", "Parameters")
        return {"Values": [date], "StorageDescriptor": {**{k: sd[k] for k in keep if k in sd},
                                                        "Location": location}}

    def sync(self, cleanup: bool = True) -> dict:
        pointers = {k: self.lake.get_json(k) or {"partitions": {}, "previous_partitions": {}}
                    for k in (SILVER_POINTER, GOLD_POINTER)}
        report = {}
        for table, (pkey, prefix) in TABLES.items():
            desired = {d: self.lake.uri(f"{prefix}/pub_date={d}/v={v}/")
                       for d, v in pointers[pkey]["partitions"].items()}
            sd = self.glue.get_table(DatabaseName=self.db, Name=table)["Table"]["StorageDescriptor"]
            existing = self._existing(table)
            create = [d for d in desired if d not in existing]
            update = [d for d in desired if d in existing and existing[d] != desired[d]]
            delete = [d for d in existing if d not in desired]
            for i in range(0, len(create), 100):
                r = self.glue.batch_create_partition(DatabaseName=self.db, TableName=table, PartitionInputList=[
                    self._partition_input(sd, d, desired[d]) for d in create[i:i + 100]])
                _raise_errors(r, table, "create")
            for i in range(0, len(update), 100):
                r = self.glue.batch_update_partition(DatabaseName=self.db, TableName=table, Entries=[
                    {"PartitionValueList": [d], "PartitionInput": self._partition_input(sd, d, desired[d])}
                    for d in update[i:i + 100]])
                _raise_errors(r, table, "update")
            for i in range(0, len(delete), 25):
                r = self.glue.batch_delete_partition(DatabaseName=self.db, TableName=table,
                                                     PartitionsToDelete=[{"Values": [d]} for d in delete[i:i + 25]])
                _raise_errors(r, table, "delete")
            report[table] = {"created": sorted(create), "updated": sorted(update), "deleted": sorted(delete),
                             "total": len(desired)}
            log.info("Glue %s: +%d ~%d -%d (total %d)", table, len(create), len(update), len(delete), len(desired))

        # Termina un outbox de Silver que haya quedado a medias
        self.lake.finalize_outbox(pointers[SILVER_POINTER].get("outbox"))
        if cleanup:
            report["deleted_old_versions"] = self._cleanup(pointers)
        return report

    def _cleanup(self, pointers: dict) -> int:
        """Borra versiones de partición que no son la vigente ni la anterior."""
        removed = 0
        for table, (pkey, prefix) in TABLES.items():
            p = pointers[pkey]
            for date_prefix in self.lake.subprefixes(f"{prefix}/"):
                date = date_prefix.rstrip("/").rsplit("pub_date=", 1)[-1]
                keep = {p["partitions"].get(date), p.get("previous_partitions", {}).get(date)}
                for vprefix in self.lake.subprefixes(date_prefix):
                    version = vprefix.rstrip("/").rsplit("v=", 1)[-1]
                    if version not in keep:
                        keys = self.lake.keys(vprefix)
                        self.lake.delete_keys(keys)
                        removed += len(keys)
        return removed


def _raise_errors(resp: dict, table: str, op: str) -> None:
    errs = resp.get("Errors") or []
    if errs:
        raise RuntimeError(f"Glue {op} en {table} falló: {errs[:3]}")
