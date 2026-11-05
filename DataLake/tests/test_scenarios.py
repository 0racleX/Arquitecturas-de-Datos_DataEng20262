"""Los 8 escenarios de la sección 4.5, de Bronze a Glue."""
import json

import pytest
from moto import mock_aws

from helpers import (BUCKET, CIENCIA, AMBIENTE, DB, FEED_AMBIENTE, FEED_CIENCIA, Pipeline, feed,
                     ingest, item, pointer, read_prefix, read_table)
from news_pipeline import scenarios
from news_pipeline.athena_checks import run_checks
from news_pipeline.bronze import BronzeStore
from news_pipeline.gold import GOLD_POINTER
from news_pipeline.lake import Lake, LockBusyError, SimulatedFailure
from news_pipeline.silver import POINTER_KEY
from news_pipeline.status import decide


@pytest.fixture
def aws():
    from helpers import setup_aws
    with mock_aws():
        yield setup_aws()


def snapshot(s3, glue):
    """Estado visible en Athena: contenido de las tablas registradas en Glue."""
    out = {}
    for t in ("silver_news", "silver_news_entities", "silver_news_key_phrases",
              "gold_news_daily", "gold_entity_daily", "gold_key_phrase_section_daily"):
        rows = read_table(s3, glue, t)
        out[t] = sorted(json.dumps({k: v for k, v in r.items() if k not in (
            "silver_version", "silver_published_at_utc", "gold_version", "built_at_utc")},
            sort_keys=True, default=str) for r in rows)
    return out


def initial(s3, glue):
    ingest(s3, CIENCIA, FEED_CIENCIA, "run1")
    ingest(s3, AMBIENTE, FEED_AMBIENTE, "run1")
    p = Pipeline(s3, glue)
    return p, p.run("pipe1")


# 1 ---------------------------------------------------------------------------
def test_carga_inicial(aws):
    s3, glue = aws
    _, out = initial(s3, glue)
    assert out["gold"]["committed"] and out["catalog"]["silver_news"]["created"] == ["2026-10-20", "2026-10-21"]
    daily = read_table(s3, glue, "gold_news_daily")
    assert sum(r["news_count_first_section"] for r in daily) == len(read_table(s3, glue, "silver_news")) == 3
    ciencia_20 = [r for r in daily if r["pub_date"] == "2026-10-20" and r["seccion"] == "Ciencia"][0]
    ambiente_20 = [r for r in daily if r["pub_date"] == "2026-10-20" and r["seccion"] == "Medio Ambiente"][0]
    assert (ciencia_20["news_count"], ciencia_20["news_count_first_section"]) == (1, 1)
    assert (ambiente_20["news_count"], ambiente_20["news_count_first_section"]) == (1, 0)  # nota en 2 feeds
    ents = {(r["pub_date"], r["entity_text"]): r for r in read_table(s3, glue, "gold_entity_daily")}
    petro = ents[("2026-10-21", "Gustavo Petro")]
    assert petro["news_count"] == 1 and petro["mention_count"] == 2    # repeticiones no inflan news_count


# 2 ---------------------------------------------------------------------------
def test_registros_defectuosos(aws):
    s3, glue = aws
    p, _ = initial(s3, glue)
    store = BronzeStore(s3, BUCKET)
    base = store.latest_success("eltiempo_ciencia")
    content, note = scenarios.build(store.read_bronze(base), "defectuoso")
    m = store.store_controlled(CIENCIA, "controlled__defectuoso__1", content, base.run_id, note)
    out = p.run("pipe2", force_run_ids=[m.run_id])
    q = [r for r in read_prefix(s3, "quarantine/silver/") if r["bronze_run_id"] == m.run_id]
    assert sorted(r["reason"] for r in q) == sorted(
        ["MISSING_LINK", "INVALID_PUB_DATE", "MISSING_TITLE", "INVALID_LINK",   # 4 agregados
         "MISSING_LINK", "INVALID_PUB_DATE", "MISSING_TITLE"])                   # 3 que ya venían
    assert all(r["bronze_uri"] == m.s3_uri for r in q)
    log = [b for b in read_prefix(s3, "control/silver/batches/") if b["bronze_run_id"] == m.run_id][0]
    assert log["duplicates_in_batch"] == 2 and log["invalid"] == 7
    assert log["items_read"] == log["invalid"] + log["duplicates_in_batch"] + log["new"] + \
        log["updated"] + log["unchanged"] + log["stale"]
    assert out["silver"]["news_current"] == 3


# 3 ---------------------------------------------------------------------------
def test_repeticion_de_lote_exitoso(aws):
    s3, glue = aws
    p, _ = initial(s3, glue)
    before = snapshot(s3, glue)
    out = Pipeline(s3, glue).run("pipe2", force_run_ids=["run1"])
    assert out["silver"]["dirty_dates"] == [] and out["gold"]["committed"] is False
    assert out["silver"]["new"] == 0 and out["silver"]["unchanged"] == 4
    assert snapshot(s3, glue) == before                                   # sin duplicados ni doble conteo
    logs = [b for b in read_prefix(s3, "control/silver/batches/") if b["bronze_run_id"] == "run1"]
    assert sorted(b["reprocessed"] for b in logs) == [False, False, True, True]


# 4 ---------------------------------------------------------------------------
def test_nuevo_lote_incremental(aws):
    s3, glue = aws
    initial(s3, glue)
    ingest(s3, CIENCIA, feed(item("Nueva noticia de Bogotá", "https://www.eltiempo.com/vida/ciencia/nueva-9",
                                  "La NASA visita Bogotá", pub="2026-10-22T07:00:00-05:00")), "run2")
    p = Pipeline(s3, glue)
    out = p.run("pipe2")
    assert out["silver"]["new"] == 1 and out["silver"]["batches"] == 1
    assert out["silver"]["dirty_dates"] == ["2026-10-22"]                 # no reescribe 20 ni 21
    assert out["gold"]["dirty_dates"] == ["2026-10-22"]
    assert p.fake.docs["ent"] == 1                                        # solo la noticia nueva
    assert out["catalog"]["silver_news"]["created"] == ["2026-10-22"]     # 8: nueva partición
    assert len(read_table(s3, glue, "silver_news")) == 4


# 5 ---------------------------------------------------------------------------
@pytest.mark.parametrize("scenario,comprehend_calls,changed", [("texto", 1, ["title"]),
                                                               ("fecha", 0, ["pub_date_utc"])])
def test_noticia_modificada(aws, scenario, comprehend_calls, changed):
    s3, glue = aws
    initial(s3, glue)
    store = BronzeStore(s3, BUCKET)
    base = store.latest_success("eltiempo_ciencia")
    content, note = scenarios.build(store.read_bronze(base), scenario)
    m = store.store_controlled(CIENCIA, f"controlled__{scenario}__1", content, base.run_id, note)
    p = Pipeline(s3, glue)
    out = p.run("pipe2", force_run_ids=[m.run_id])
    assert out["silver"]["updated"] == 1 and p.fake.docs["ent"] == comprehend_calls
    news = read_table(s3, glue, "silver_news")
    upd = [n for n in news if n["version"] == 2]
    assert len(upd) == 1 and len(news) == 3 and upd[0]["bronze_run_id"] == m.run_id
    hist = sorted((h for h in read_prefix(s3, "silver/news_history/") if h["news_id"] == upd[0]["news_id"]),
                  key=lambda h: h["version"])
    assert [h["change_type"] for h in hist] == ["NEW", "UPDATED"] and hist[1]["changed_fields"] == changed
    assert hist[1]["text_changed"] is (scenario == "texto")


# 6 ---------------------------------------------------------------------------
def test_fallo_controlado_y_reejecucion_silver(aws):
    s3, glue = aws
    initial(s3, glue)
    before = snapshot(s3, glue)
    ingest(s3, CIENCIA, feed(item("Otra noticia", "https://www.eltiempo.com/vida/ciencia/otra",
                                  pub="2026-10-22T07:00:00-05:00")), "run2")
    p = Pipeline(s3, glue)
    p.silver.prepare("pipe2")
    p.silver.enrich("pipe2")
    with pytest.raises(SimulatedFailure):
        p.silver.publish("pipe2", simulate_failure=True)
    assert pointer(s3)["run_id"] == "pipe1"                              # sin commit
    p.catalog.sync()
    assert snapshot(s3, glue) == before                                   # nada parcial visible
    assert not s3.list_objects_v2(Bucket=BUCKET, Prefix="control/locks/").get("KeyCount")
    # Reintento (Airflow "Clear" de la tarea): misma ejecución, sin duplicar
    out = p.silver.publish("pipe2")
    p.gold.build("pipe2")
    p.catalog.sync()
    assert out["committed"] and out["new"] == 1
    assert len(read_table(s3, glue, "silver_news")) == 4
    assert len([b for b in read_prefix(s3, "control/silver/batches/") if b["bronze_run_id"] == "run2"]) == 1
    assert p.silver.publish("pipe2")["already_published"]                # reintentar otra vez no hace nada


def test_fallo_controlado_gold(aws):
    s3, glue = aws
    initial(s3, glue)
    ingest(s3, CIENCIA, feed(item("Otra", "https://www.eltiempo.com/vida/ciencia/otra",
                                  pub="2026-10-22T07:00:00-05:00")), "run2")
    p = Pipeline(s3, glue)
    p.silver.prepare("pipe2"); p.silver.enrich("pipe2"); p.silver.publish("pipe2")  # noqa: E702
    with pytest.raises(SimulatedFailure):
        p.gold.build("pipe2", simulate_failure=True)
    assert pointer(s3, GOLD_POINTER)["run_id"] == "pipe1"
    out = p.gold.build("pipe2")
    assert out["dirty_dates"] == ["2026-10-22"]
    p.catalog.sync()
    assert sum(r["news_count_first_section"] for r in read_table(s3, glue, "gold_news_daily")) == 4


def test_caida_entre_commit_y_outbox_se_recupera(aws):
    s3, glue = aws
    ingest(s3, CIENCIA, FEED_CIENCIA, "run1")
    p = Pipeline(s3, glue)
    p.silver.prepare("pipe1"); p.silver.enrich("pipe1")  # noqa: E702
    real = p.silver.lake.finalize_outbox
    p.silver.lake.finalize_outbox = lambda outbox: (_ for _ in ()).throw(RuntimeError("EC2 apagada"))
    with pytest.raises(RuntimeError):
        p.silver.publish("pipe1")
    p.silver.lake.finalize_outbox = real
    assert pointer(s3)["run_id"] == "pipe1"
    assert not read_prefix(s3, "control/silver/batches/")                # marcadores aún en work/
    p.silver.lake.release_lock("datalake-writer", "pipe1")
    p.catalog.sync()                                                      # el siguiente paso completa el outbox
    assert len(read_prefix(s3, "control/silver/batches/")) == 1
    assert read_prefix(s3, "quarantine/silver/") and read_prefix(s3, "silver/news_history/")


def test_lock_impide_escrituras_concurrentes(aws):
    s3, _ = aws
    s3.put_object(Bucket=BUCKET, Key="probe", Body=b"1", IfNoneMatch="*")
    try:
        s3.put_object(Bucket=BUCKET, Key="probe", Body=b"1", IfNoneMatch="*")
        pytest.skip("Esta versión de moto no simula If-None-Match (S3 real sí); use moto>=5.1")
    except Exception:
        pass
    lake = Lake(s3, BUCKET)
    lake.acquire_lock("datalake-writer", "run-A")
    with pytest.raises(LockBusyError):
        lake.acquire_lock("datalake-writer", "run-B")
    lake.acquire_lock("datalake-writer", "run-A")                         # re-entrante
    lake.release_lock("datalake-writer", "run-A")
    lake.acquire_lock("datalake-writer", "run-B")


# 7 ---------------------------------------------------------------------------
def test_reprocesamiento_desde_bronze(aws):
    s3, glue = aws
    initial(s3, glue)
    before = snapshot(s3, glue)
    p = Pipeline(s3, glue)
    out = p.run("rebuild1", full_refresh=True)
    assert out["silver"]["batches"] == 2 and out["silver"]["dirty_dates"] == []
    assert p.fake.docs == {"lang": 0, "ent": 0, "kp": 0}                  # ni RSS ni Comprehend
    assert snapshot(s3, glue) == before


# 8 + limpieza ---------------------------------------------------------------
def test_versiones_viejas_se_limpian(aws):
    s3, glue = aws
    initial(s3, glue)
    for i in range(3):
        ingest(s3, CIENCIA, feed(item(f"Extra {i}", f"https://www.eltiempo.com/vida/ciencia/e{i}",
                                      pub="2026-10-20T11:00:00-05:00")), f"run-x{i}")
        Pipeline(s3, glue).run(f"pipe-x{i}")
    versions = {k.split("/v=")[1].split("/")[0] for k in [
        o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="silver/news/pub_date=2026-10-20/")["Contents"]]}
    assert versions == {"pipe-x1", "pipe-x2"}                             # vigente + anterior


# Validaciones y estado -------------------------------------------------------
class FakeRunner:
    database = DB

    def __init__(self, fail=None):
        self.fail, self.sql = fail or {}, []

    def run(self, sql):
        self.sql.append(sql)
        name = next((n for n in self.fail if n in sql), None)
        return {"query_id": f"q{len(self.sql)}", "rows": [{"failures": str(self.fail.get(name, 0)), "n": "3"}],
                "bytes_scanned": 1000 if "WHERE pub_date" in sql else 5000}


def test_athena_checks_and_status(aws):
    s3, _ = aws
    lake = Lake(s3, BUCKET)
    ok = run_checks(FakeRunner(), lake, "pipe1", "2026-10-21")
    assert ok["passed"] and ok["pruning"]["con_filtro_particion"]["bytes_scanned"] < \
        ok["pruning"]["sin_filtro_particion"]["bytes_scanned"]
    assert len(read_prefix(s3, "control/quality/")) == ok["checks"]
    bad = run_checks(FakeRunner({"count(*) - count(DISTINCT news_id)": 2}), lake, "pipe2", None)
    assert bad["failed_checks"] == ["news_id_unico"]

    src_ok = [{"source_id": "a", "status": "SUCCESS"}, {"source_id": "b", "status": "SUCCESS"}]
    src_part = [{"source_id": "a", "status": "SUCCESS"}, {"source_id": "b", "status": "HTTP_ERROR"}]
    assert decide("extract", src_ok, [], ok)[0] == "COMPLETE"
    assert decide("extract", src_part, [], ok)[0] == "PARTIAL"
    assert decide("extract", [{"source_id": "b", "status": "HTTP_ERROR"}], [], ok)[0] == "FAILED"
    assert decide("extract", src_ok, ["silver_publish"], ok)[0] == "FAILED"
    assert decide("reprocess", [], [], bad)[0] == "FAILED"


def test_scenario_builders_keep_xml_valid():
    from news_pipeline.rss_parser import parse_feed
    for sc in ("texto", "fecha", "defectuoso"):
        content, _ = scenarios.build(FEED_CIENCIA, sc)
        items = parse_feed(content, source_id="s", medio="m", seccion="x", source_url="u",
                           extracted_at_utc="2026-11-01T00:00:00Z", run_id="r")
        assert len(items) == (11 if sc == "defectuoso" else 6)
    assert POINTER_KEY
