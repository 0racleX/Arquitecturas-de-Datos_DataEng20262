"""Ejecución manual de los pasos sin Airflow (útil para depurar desde la EC2).

  py -m news_pipeline.cli ingest  --bucket $DATALAKE_BUCKET
  py -m news_pipeline.cli silver  --bucket $DATALAKE_BUCKET [--full-refresh] [--force-run-id <id>]
  py -m news_pipeline.cli gold    --bucket $DATALAKE_BUCKET
  py -m news_pipeline.cli catalog --bucket $DATALAKE_BUCKET
  py -m news_pipeline.cli all     --bucket $DATALAKE_BUCKET      (ingest + silver + gold + catalog)

El camino normal es el DAG news_datalake_pipeline; esto es un respaldo.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.config import Config


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["ingest", "silver", "gold", "catalog", "all"])
    p.add_argument("--bucket", default=os.environ.get("DATALAKE_BUCKET"))
    p.add_argument("--config", default=os.environ.get("NEWS_CONFIG_PATH", "config/sources.yaml"))
    p.add_argument("--glue-database", default=os.environ.get("GLUE_DATABASE", "news_datalake"))
    p.add_argument("--run-id")
    p.add_argument("--full-refresh", action="store_true")
    p.add_argument("--force-run-id", action="append", default=[])
    a = p.parse_args()
    if not a.bucket:
        p.error("--bucket (o DATALAKE_BUCKET) es obligatorio")

    from .bronze import BronzeStore
    from .catalog import CatalogSync
    from .comprehend_enricher import ComprehendEnricher
    from .config import load_config
    from .gold import GoldBuilder
    from .silver import SilverProcessor

    cfg = load_config(a.config)
    retry = Config(retries={"mode": "adaptive", "max_attempts": 10})
    s3 = boto3.client("s3", config=retry)
    run_id = a.run_id or f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    out = {"run_id": run_id}

    if a.command in ("ingest", "all"):
        store = BronzeStore(s3, a.bucket)
        out["ingest"] = [{"source": s.source_id, **{k: v for k, v in store.ingest(s, run_id, cfg.http).__dict__.items()
                                                   if k in ("status", "http_status", "item_count", "s3_uri", "error")}}
                         for s in cfg.enabled_sources()]
    if a.command in ("silver", "all"):
        enr = ComprehendEnricher(boto3.client("comprehend", config=retry), cfg.comprehend)
        out["silver"] = SilverProcessor(s3, a.bucket, cfg.silver, enr).run(
            run_id, full_refresh=a.full_refresh, force_run_ids=a.force_run_id)
    if a.command in ("gold", "all"):
        out["gold"] = GoldBuilder(s3, a.bucket).build(run_id)
    if a.command in ("catalog", "all"):
        out["catalog"] = CatalogSync(boto3.client("glue", config=retry), s3, a.bucket, a.glue_database).sync()
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
