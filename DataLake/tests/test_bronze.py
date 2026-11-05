import json

import boto3
import pytest
from moto import mock_aws

from news_pipeline import bronze as B
from news_pipeline.config import HttpConfig, Source, load_config
from news_pipeline.http_client import FetchResult
from news_pipeline.rss_parser import parse_pub_date

BUCKET = "test-news-datalake"
SRC = Source("eltiempo_ciencia", "El Tiempo", "Ciencia", "https://www.eltiempo.com/rss/vida_ciencia.xml")
HTTP = HttpConfig()

FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:slash="http://purl.org/rss/1.0/modules/slash/">
<channel><title>Ciencia</title>
 <item><title>Nueva particula</title><pubDate>2026-10-20T09:54:20-05:00</pubDate>
  <link>https://www.eltiempo.com/vida/ciencia/a-1</link><guid>https://www.eltiempo.com/vida/ciencia/a-1</guid>
  <description>&lt;p&gt;Texto con &#225;cento&lt;/p&gt;</description><slash:comments>0</slash:comments></item>
 <item><title>Sin guid</title><pubDate>Wed, 21 Oct 2026 07:23:35 -0500</pubDate>
  <link>https://www.eltiempo.com/vida/ciencia/a-2</link><description>Otra</description></item>
</channel></rss>"""


class FakeFetcher:
    def __init__(self, result):
        self.result, self.calls = result, 0

    def __call__(self, url, **kw):
        self.calls += 1
        return self.result


def ok(content=FEED):
    return FetchResult(url=SRC.url, http_status=200, content=content,
                       content_type="application/xml", attempts=1)


@pytest.fixture
def store():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield B.BronzeStore(s3, BUCKET)


def keys(store, prefix=""):
    r = store.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return sorted(o["Key"] for o in r.get("Contents", []))


def test_successful_ingest_writes_bronze_staging_and_manifest(store):
    m = store.ingest(SRC, "scheduled__2026-10-22T11:00:00+00:00", HTTP,
                     ingestion_date="2026-10-22", fetcher=FakeFetcher(ok()))
    rid = "scheduled__2026-10-22T11-00-00-00-00"
    assert m.status == B.SUCCESS and m.item_count == 2 and m.http_status == 200
    assert m.content_sha256 == B.sha256(FEED)
    bkey = f"bronze/source=eltiempo_ciencia/ingestion_date=2026-10-22/run_id={rid}/feed.xml"
    assert m.s3_uri == f"s3://{BUCKET}/{bkey}"
    # Bronze conserva el original byte a byte
    assert store.s3.get_object(Bucket=BUCKET, Key=bkey)["Body"].read() == FEED
    items = [json.loads(l) for l in store._get(B.staging_key(SRC.source_id, "2026-10-22", rid)).splitlines()]
    assert items[0]["item_id"] == "https://www.eltiempo.com/vida/ciencia/a-1"
    assert items[0]["pub_date_utc"] == "2026-10-20T14:54:20Z"
    assert items[1]["item_id"] == "https://www.eltiempo.com/vida/ciencia/a-2"   # sin guid -> link
    assert items[0]["source_id"] == "eltiempo_ciencia" and items[0]["extracted_at_utc"]


def test_rerun_same_run_is_idempotent_and_does_not_refetch(store):
    f = FakeFetcher(ok())
    store.ingest(SRC, "run1", HTTP, fetcher=f)
    before = keys(store)
    store.ingest(SRC, "run1", HTTP, fetcher=f)
    assert f.calls == 1
    assert keys(store) == before


def test_ingestion_date_is_pinned_per_run_id(store):
    store.ingest(SRC, "run1", HTTP, fetcher=FakeFetcher(FetchResult(SRC.url, 500, b"", error="HTTP 500")))
    first = B.json.loads(store._get(B.run_index_key("run1")))["ingestion_date"]
    m = store.ingest(SRC, "run1", HTTP, fetcher=FakeFetcher(ok()))   # reintento
    assert m.ingestion_date == first and m.status == B.SUCCESS
    assert len(keys(store, "bronze/")) == 1


@pytest.mark.parametrize("res,status,quarantined", [
    (FetchResult(SRC.url, 503, b"down", error="HTTP 503"), B.HTTP_ERROR, False),
    (FetchResult(SRC.url, None, b"", error="ConnectTimeout"), B.NETWORK_ERROR, False),
    (FetchResult(SRC.url, 200, b"   "), B.EMPTY, True),
    (FetchResult(SRC.url, 200, b"<rss><channel><item><title>x</rss>"), B.MALFORMED, True),
    (FetchResult(SRC.url, 200, b"<html><body>no feed</body></html>"), B.MALFORMED, True),
    (FetchResult(SRC.url, 200, b"<rss version='2.0'><channel></channel></rss>"), B.EMPTY, True),
])
def test_failures_are_logged_but_never_written_to_bronze(store, res, status, quarantined):
    m = store.ingest(SRC, "runX", HTTP, ingestion_date="2026-10-22", fetcher=FakeFetcher(res))
    assert m.status == status and m.s3_uri is None and m.error
    assert keys(store, "bronze/") == []
    assert store.get_manifest(SRC.source_id, "2026-10-22", "runX").status == status
    assert (len(keys(store, "quarantine/")) == 1) == (quarantined and bool(res.content))


def test_xxe_payload_is_rejected(store):
    evil = b"""<?xml version="1.0"?><!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]>
    <rss><channel><item><title>&x;</title></item></channel></rss>"""
    m = store.ingest(SRC, "evil", HTTP, fetcher=FakeFetcher(ok(evil)))
    assert m.status == B.MALFORMED


def test_reprocess_reads_bronze_without_fetching_and_creates_no_copy(store):
    store.ingest(SRC, "run1", HTTP, ingestion_date="2026-10-22", fetcher=FakeFetcher(ok()))
    before = keys(store, "bronze/") + keys(store, "staging/")
    m, items = store.reprocess(SRC.source_id, "run1", reprocess_id="rp1")
    m2, _ = store.reprocess(SRC.source_id, "run1", reprocess_id="rp2")
    assert len(items) == 2 and m2.reprocess_count == 2
    assert keys(store, "bronze/") + keys(store, "staging/") == before   # mismas claves
    assert len(keys(store, "control/ingestion_log/")) == 1               # un solo lote lógico
    assert len(keys(store, "control/reprocess_log/")) == 2               # auditoría


def test_reprocess_detects_corruption(store):
    store.ingest(SRC, "run1", HTTP, ingestion_date="2026-10-22", fetcher=FakeFetcher(ok()))
    store.s3.put_object(Bucket=BUCKET, Key=B.bronze_key(SRC.source_id, "2026-10-22", "run1"), Body=b"x")
    with pytest.raises(B.IntegrityError):
        store.reprocess(SRC.source_id, "run1")


def test_reprocess_unknown_batch(store):
    with pytest.raises(B.BatchNotFoundError):
        store.reprocess(SRC.source_id, "nope")


def test_unchanged_feed_is_flagged(store):
    store.ingest(SRC, "run1", HTTP, fetcher=FakeFetcher(ok()))
    m = store.ingest(SRC, "run2", HTTP, fetcher=FakeFetcher(ok()))
    assert m.content_unchanged and m.previous_run_id == "run1"


def test_pub_date_formats():
    assert parse_pub_date("2026-10-20T09:54:20-05:00") == "2026-10-20T14:54:20Z"
    assert parse_pub_date("Wed, 21 Oct 2026 07:23:35 -0500") == "2026-10-21T12:23:35Z"
    assert parse_pub_date("ayer") is None and parse_pub_date(None) is None


def test_config_rejects_directory_page(tmp_path):
    p = tmp_path / "s.yaml"
    p.write_text("sources:\n - {source_id: x, medio: m, seccion: s, url: 'https://www.eltiempo.com/rss'}\n")
    with pytest.raises(ValueError, match="no parece un feed"):
        load_config(p)


def test_real_config_loads():
    cfg = load_config("config/sources.yaml")
    assert [s.source_id for s in cfg.enabled_sources()] == [
        "eltiempo_medio_ambiente", "eltiempo_ciencia", "eltiempo_viajar"]
