"""Limpieza, identificación, enriquecimiento y publicación de Silver."""
import pytest
from moto import mock_aws

from helpers import (AMBIENTE, CIENCIA, FEED_AMBIENTE, FEED_CIENCIA, FakeComprehend, Pipeline,
                     feed, ingest, item, read_prefix, read_table)
from news_pipeline import bronze as B
from news_pipeline.cleaning import build_identity, canonical_url, clean_html


@pytest.fixture
def aws():
    from helpers import setup_aws
    with mock_aws():
        yield setup_aws()


def test_clean_html_handles_double_escaping_and_scripts():
    assert clean_html("&lt;p&gt;Hola&amp;nbsp;&lt;b&gt;mundo&lt;/b&gt;&lt;/p&gt;") == "Hola mundo"
    assert clean_html("<p>Uno</p><p>Dos</p><script>alert(1)</script>") == "Uno Dos"
    assert clean_html("Caf&eacute; &amp; t&#233;") == "Café & té"
    assert clean_html("   ") is None and clean_html(None) is None


def test_canonical_url_and_identity():
    assert canonical_url("http://WWW.eltiempo.com/vida/x-1/?utm_source=rss&b=2&a=1#top") == \
        "https://www.eltiempo.com/vida/x-1?a=1&b=2"
    i1 = build_identity(guid="https://www.eltiempo.com/a?utm_medium=x", link=None, title="t",
                        pub_date_utc=None, medio="El Tiempo")
    i2 = build_identity(guid=None, link="https://www.eltiempo.com/a/", title="t", pub_date_utc=None,
                        medio="El Tiempo")
    assert i1[0] == i2[0] and (i1[1], i2[1]) == ("guid_url", "link")
    assert build_identity(guid="abc-1", link="https://x.com/a", title="t", pub_date_utc=None,
                          medio="El Tiempo")[1] == "guid"
    assert build_identity(guid=None, link=None, title="T", pub_date_utc="2026-01-01T00:00:00Z",
                          medio="El Tiempo")[1] == "title_date_hash"


def test_silver_content_and_partitions(aws):
    s3, glue = aws
    ingest(s3, CIENCIA, FEED_CIENCIA, "run1")
    ingest(s3, AMBIENTE, FEED_AMBIENTE, "run1")
    p = Pipeline(s3, glue)
    out = p.run("pipe1")["silver"]
    assert out["committed"] and out["invalid"] == 3 and out["duplicates_in_batch"] == 1
    assert out["cross_feed_matches"] == 1 and out["news_current"] == 3
    assert sorted(out["dirty_dates"]) == ["2026-10-20", "2026-10-21"]

    news = {n["title"]: n for n in read_table(s3, glue, "silver_news")}
    a = news["La NASA y Bogotá firman acuerdo"]
    assert a["pub_date"] == "2026-10-20" and a["description"] == "Gustavo Petro celebró el acuerdo con la NASA."
    assert "<p>" in a["description_raw"] and a["pub_date_raw"] == "2026-10-20T09:54:20-05:00"
    assert a["pub_date_utc"].isoformat() == "2026-10-20T14:54:20+00:00"
    assert sorted(a["source_ids"]) == ["eltiempo_ciencia", "eltiempo_medio_ambiente"] and a["n_sources"] == 2
    assert a["bronze_uri"].startswith("s3://test-news-datalake/bronze/source=eltiempo_ciencia/")
    assert a["nlp_status"] == "ANALYZED"
    assert news["Ceci est un article"]["nlp_status"] == "SKIPPED_UNSUPPORTED_LANGUAGE"

    ents = read_table(s3, glue, "silver_news_entities")
    petro = [e for e in ents if e["news_id"] == news["Hallazgo en el Amazonas"]["news_id"]
             and e["entity_type"] == "PERSON"]
    assert len(petro) == 1 and petro[0]["mention_count"] == 2           # entidad DISTINTA por noticia
    assert {e["entity_category"] for e in ents} == {"persona", "organizacion", "lugar"}
    assert not any(e["entity_type"] == "DATE" for e in ents)
    assert p.fake.docs["ent"] == 2                                       # el francés no llega a entidades


def test_empty_long_and_error_texts_have_explicit_status(aws):
    s3, glue = aws
    ingest(s3, CIENCIA, feed(item("Sí", "https://www.eltiempo.com/a", desc=""),
                             item("Largo", "https://www.eltiempo.com/b", desc="palabra " * 1500),
                             item("Fallará", "https://www.eltiempo.com/c", desc="texto boom")), "run1")
    p = Pipeline(s3, glue, client=FakeComprehend(fail_texts=["boom"]))
    p.run("pipe1")
    status = {n["title"]: n["nlp_status"] for n in read_table(s3, glue, "silver_news")}
    assert status == {"Sí": "SKIPPED_EMPTY", "Largo": "ANALYZED_TRUNCATED", "Fallará": "ERROR"}
    cache = {r["status"]: r for r in read_prefix(s3, "silver/nlp_analysis/")}
    assert "ERROR" not in cache                                          # los errores no se cachean
    assert len(cache["ANALYZED_TRUNCATED"]["analyzed_text"].encode()) <= 4900

    # Siguiente ejecución: solo se reintenta el que falló
    p2 = Pipeline(s3, glue)
    out = p2.run("pipe2")
    assert p2.fake.docs["ent"] == 1 and out["silver"]["committed"]
    assert {n["title"]: n["nlp_status"] for n in read_table(s3, glue, "silver_news")}["Fallará"] == "ANALYZED"


def test_tampered_bronze_is_detected(aws):
    s3, glue = aws
    m = ingest(s3, CIENCIA, FEED_CIENCIA, "run1")
    s3.put_object(Bucket="test-news-datalake", Key=m.s3_uri.split("test-news-datalake/")[1], Body=b"<rss/>")
    with pytest.raises(B.IntegrityError):
        Pipeline(s3, glue).run("pipe1")
