"""DAG news_datalake_pipeline: RSS -> Bronze -> Silver -> Gold -> Glue -> Athena (4.6).

Pasos (dependencias explícitas):
  1. validate_environment        configuración y acceso a S3, Glue, Athena y Comprehend
  2. extract_source (x fuente)   consulta cada feed y escribe en Bronze
  3. silver_clean_validate_dedup limpieza, validación, deduplicación y cambios
  4. silver_enrich_comprehend    Comprehend solo para textos nuevos o modificados
  5. silver_publish              commit atómico de Silver (particiones por fecha)
  6. gold_build                  agregados solo para las fechas que cambiaron
  7. glue_update_catalog         crea/actualiza/borra particiones en Glue
  8. athena_validate             validaciones finales con Athena
  9. record_status               COMPLETE / PARTIAL / FAILED en control/pipeline_runs/

Modos (parámetro `mode` al disparar el DAG):
  extract    (programado) consulta los RSS y procesa los lotes pendientes
  reprocess  NO consulta los RSS; vuelve a procesar el lote `reprocess_run_id` desde Bronze
  rebuild    NO consulta los RSS; reconstruye Silver y Gold desde TODO Bronze

Fallo controlado (demostración):  airflow variables set news_simulate_failure silver_publish
  (valores: silver_publish | gold_build). Se borra con: airflow variables delete news_simulate_failure
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.models.param import Param

PIPELINE_HOME = os.environ.get("NEWS_PIPELINE_HOME", "/opt/news-datalake")
sys.path.insert(0, os.path.join(PIPELINE_HOME, "src"))

BUCKET = os.environ.get("DATALAKE_BUCKET", "")
CONFIG_PATH = os.environ.get("NEWS_CONFIG_PATH", os.path.join(PIPELINE_HOME, "config", "sources.yaml"))
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "news_datalake")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "news-datalake")
SCHEDULE = os.environ.get("NEWS_SCHEDULE", "0 * * * *")
WRITER_POOL = "datalake_writer"      # 1 slot: una sola tarea escribe Silver/Gold/Glue a la vez

log = logging.getLogger("news_pipeline.dag")


def _tag(context, source: str | None = None) -> str:
    """Prefijo de log con lote, tarea y fuente."""
    s = f"[run_id={context['run_id']} task={context['ti'].task_id}"
    return s + (f" source={source}]" if source else "]")


def _mode(context) -> str:
    return context["params"]["mode"]


def _simulate(step: str) -> bool:
    return Variable.get("news_simulate_failure", default_var="none") == step


def _clients():
    import boto3
    from botocore.config import Config
    retry = Config(retries={"mode": "adaptive", "max_attempts": 10})
    return {"s3": boto3.client("s3", config=retry), "glue": boto3.client("glue", config=retry),
            "athena": boto3.client("athena", config=retry),
            "comprehend": boto3.client("comprehend", config=retry)}


@dag(
    dag_id="news_datalake_pipeline",
    description="Data lake de noticias RSS: Bronze/Silver/Gold + Comprehend + Glue + Athena",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,                       # control de concurrencia a nivel de DAG
    dagrun_timeout=timedelta(hours=1),
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=1),
                  "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(minutes=30)},
    params={
        "mode": Param("extract", enum=["extract", "reprocess", "rebuild"],
                      description="extract: consulta RSS | reprocess: un lote desde Bronze | rebuild: todo desde Bronze"),
        "reprocess_run_id": Param("", type="string", description="run_id del lote Bronze (solo mode=reprocess)"),
    },
    tags=["news", "datalake", "bronze", "silver", "gold"],
)
def news_datalake_pipeline():

    @task
    def validate_environment(**context) -> list[str]:
        from news_pipeline.athena_checks import AthenaRunner  # noqa: F401  (valida import)
        from news_pipeline.config import load_config
        tag = _tag(context)
        if not BUCKET:
            raise AirflowFailException("Falta DATALAKE_BUCKET en el entorno")
        cfg = load_config(CONFIG_PATH)                         # valida URLs y parámetros
        p = context["params"]
        if p["mode"] == "reprocess" and not p["reprocess_run_id"]:
            raise AirflowFailException("mode=reprocess requiere reprocess_run_id")
        c = _clients()
        c["s3"].head_bucket(Bucket=BUCKET)
        c["s3"].put_object(Bucket=BUCKET, Key="control/healthcheck/last.txt", Body=context["run_id"].encode())
        c["glue"].get_database(Name=GLUE_DATABASE)
        for t in ("silver_news", "silver_news_entities", "silver_news_key_phrases", "gold_news_daily",
                  "gold_entity_daily", "gold_key_phrase_section_daily"):
            c["glue"].get_table(DatabaseName=GLUE_DATABASE, Name=t)
        c["athena"].get_work_group(WorkGroup=ATHENA_WORKGROUP)
        if cfg.comprehend.enabled:
            c["comprehend"].detect_dominant_language(Text="Validación de acceso a Amazon Comprehend")
        sources = [s.source_id for s in cfg.enabled_sources()]
        log.info("%s configuración válida; fuentes=%s modo=%s", tag, sources, p["mode"])
        return sources

    @task(max_active_tis_per_dagrun=3)
    def extract_source(source_id: str, **context) -> dict:
        if _mode(context) != "extract":
            raise AirflowSkipException(f"Modo {_mode(context)}: no se consultan los RSS")
        from news_pipeline.bronze import SUCCESS, BronzeStore
        from news_pipeline.config import load_config
        cfg = load_config(CONFIG_PATH)
        m = BronzeStore(_clients()["s3"], BUCKET).ingest(cfg.get(source_id), context["run_id"], cfg.http)
        tag = _tag(context, source_id)
        log.info("%s status=%s http=%s items=%s sha256=%s uri=%s", tag, m.status, m.http_status,
                 m.item_count, m.content_sha256, m.s3_uri)
        if m.status != SUCCESS:
            # Se registra el lote fallido y se reintenta; si sigue fallando, record_status
            # marcará la ejecución como PARTIAL (no como "fuente sin noticias").
            raise RuntimeError(f"{tag} fuente no disponible: {m.status} {m.error}")
        return {"source_id": source_id, "status": m.status, "items": m.item_count}

    @task(trigger_rule="all_done")
    def silver_clean_validate_dedup(**context) -> dict:
        ti = context["dag_run"].get_task_instance("validate_environment")
        if ti is None or ti.state != "success":
            raise AirflowFailException("validate_environment no terminó bien; no se procesa")
        from news_pipeline.config import load_config
        from news_pipeline.silver import SilverProcessor
        cfg = load_config(CONFIG_PATH)
        p = context["params"]
        proc = SilverProcessor(_clients()["s3"], BUCKET, cfg.silver)
        out = proc.prepare(context["run_id"], full_refresh=p["mode"] == "rebuild",
                           force_run_ids=[p["reprocess_run_id"]] if p["mode"] == "reprocess" else [])
        log.info("%s %s", _tag(context), out)
        return out

    @task
    def silver_enrich_comprehend(prepared: dict, **context) -> dict:
        from news_pipeline.comprehend_enricher import ComprehendEnricher
        from news_pipeline.config import load_config
        from news_pipeline.silver import SilverProcessor
        cfg = load_config(CONFIG_PATH)
        c = _clients()
        proc = SilverProcessor(c["s3"], BUCKET, cfg.silver, ComprehendEnricher(c["comprehend"], cfg.comprehend))
        out = proc.enrich(context["run_id"])
        log.info("%s %s", _tag(context), out)
        return out

    @task(pool=WRITER_POOL)
    def silver_publish(enriched: dict, **context) -> dict:
        from news_pipeline.comprehend_enricher import ComprehendEnricher
        from news_pipeline.config import load_config
        from news_pipeline.lake import SimulatedFailure
        from news_pipeline.silver import SilverProcessor
        cfg = load_config(CONFIG_PATH)
        c = _clients()
        proc = SilverProcessor(c["s3"], BUCKET, cfg.silver, ComprehendEnricher(c["comprehend"], cfg.comprehend))
        try:
            out = proc.publish(context["run_id"], simulate_failure=_simulate("silver_publish"))
        except SimulatedFailure as exc:
            raise AirflowFailException(f"{_tag(context)} {exc}")   # sin reintentos: fallo controlado
        log.info("%s %s", _tag(context), out)
        return out

    @task(pool=WRITER_POOL)
    def gold_build(silver: dict, **context) -> dict:
        from news_pipeline.gold import GoldBuilder
        from news_pipeline.lake import SimulatedFailure
        try:
            out = GoldBuilder(_clients()["s3"], BUCKET).build(
                context["run_id"], simulate_failure=_simulate("gold_build"))
        except SimulatedFailure as exc:
            raise AirflowFailException(f"{_tag(context)} {exc}")
        log.info("%s %s", _tag(context), out)
        return out

    @task(pool=WRITER_POOL)
    def glue_update_catalog(gold: dict, **context) -> dict:
        from news_pipeline.catalog import CatalogSync
        c = _clients()
        out = CatalogSync(c["glue"], c["s3"], BUCKET, GLUE_DATABASE).sync()
        log.info("%s %s", _tag(context), out)
        return out

    @task
    def athena_validate(catalog: dict, **context) -> dict:
        from news_pipeline.athena_checks import AthenaRunner, run_checks
        from news_pipeline.lake import Lake
        from news_pipeline.silver import POINTER_KEY
        c = _clients()
        lake = Lake(c["s3"], BUCKET)
        dates = sorted((lake.get_json(POINTER_KEY) or {}).get("partitions", {}))
        out = run_checks(AthenaRunner(c["athena"], ATHENA_WORKGROUP, GLUE_DATABASE), lake,
                         context["run_id"], dates[-1] if dates else None)
        log.info("%s passed=%s fallidas=%s pruning=%s", _tag(context), out["passed"],
                 out["failed_checks"], out["pruning"])
        out.pop("results")
        if not out["passed"]:
            raise AirflowFailException(f"Validaciones fallidas: {out['failed_checks']}")
        return out

    @task(trigger_rule="all_done")
    def record_status(sources: list[str], **context) -> dict:
        from news_pipeline.bronze import BronzeStore
        from news_pipeline.lake import Lake
        from news_pipeline.status import COMPLETE, record, source_results
        dr, ti = context["dag_run"], context["ti"]
        failed = sorted({t.task_id for t in dr.get_task_instances()
                         if t.task_id not in ("extract_source", "record_status")
                         and t.state in ("failed", "upstream_failed")})
        s3 = _clients()["s3"]
        mode = _mode(context)
        src = source_results(BronzeStore(s3, BUCKET), context["run_id"], sources or []) if mode == "extract" else []
        pull = lambda tid: ti.xcom_pull(task_ids=tid)  # noqa: E731
        quality = pull("athena_validate")
        if quality is None and "athena_validate" in failed:
            quality = {"passed": False, "failed_checks": ["ver logs de athena_validate"]}
        rec = record(Lake(s3, BUCKET), run_id=context["run_id"], mode=mode, sources=src, failed_tasks=failed,
                     silver=pull("silver_publish"), gold=pull("gold_build"),
                     catalog=pull("glue_update_catalog"), quality=quality)
        log.info("%s estado=%s %s", _tag(context), rec["status"], rec["message"])
        if rec["status"] != COMPLETE:
            raise AirflowFailException(f"Ejecución {rec['status']}: {rec['message']}")
        return rec

    sources = validate_environment()
    extracted = extract_source.expand(source_id=sources)
    prepared = silver_clean_validate_dedup()
    extracted >> prepared
    validated = athena_validate(glue_update_catalog(gold_build(silver_publish(silver_enrich_comprehend(prepared)))))
    validated >> record_status(sources)


news_datalake_pipeline()
