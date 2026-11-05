"""Utilidades compartidas por las pruebas: feeds de ejemplo, Comprehend falso y fixtures AWS."""
import json

import boto3

from news_pipeline import schemas as S
from news_pipeline.bronze import BronzeStore
from news_pipeline.catalog import TABLES, CatalogSync
from news_pipeline.comprehend_enricher import ComprehendConfig, ComprehendEnricher
from news_pipeline.config import HttpConfig, SilverConfig, Source
from news_pipeline.gold import GOLD_POINTER, GoldBuilder
from news_pipeline.http_client import FetchResult
from news_pipeline.silver import POINTER_KEY, SilverProcessor

BUCKET = "test-news-datalake"
DB = "news_datalake"
CIENCIA = Source("eltiempo_ciencia", "El Tiempo", "Ciencia", "https://www.eltiempo.com/rss/vida_ciencia.xml")
AMBIENTE = Source("eltiempo_medio_ambiente", "El Tiempo", "Medio Ambiente",
                  "https://www.eltiempo.com/rss/vida_medio-ambiente.xml")


def item(title, link, desc="Descripcion", pub="2026-10-20T09:54:20-05:00", guid=True):
    g = f"<guid>{link}</guid>" if (guid and link) else ""
    lk = f"<link>{link}</link>" if link else ""
    p = f"<pubDate>{pub}</pubDate>" if pub else ""
    return f"<item><title>{title}</title>{p}{lk}{g}<description>{desc}</description></item>"


def feed(*items):
    return ('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>t</title>'
            + "".join(items) + "</channel></rss>").encode()


ACUERDO = item("La NASA y Bogotá firman acuerdo", "https://www.eltiempo.com/vida/ciencia/acuerdo-123?utm_source=rss",
               "&lt;p&gt;Gustavo Petro celebró el &lt;b&gt;acuerdo&lt;/b&gt; con la NASA.&lt;/p&gt;")
HALLAZGO = item("Hallazgo en el Amazonas", "https://www.eltiempo.com/vida/ciencia/hallazgo-1",
                "Científicos de la Universidad Nacional en el Amazonas. Gustavo Petro y Gustavo Petro.",
                pub="2026-10-21T08:00:00-05:00")
FEED_CIENCIA = feed(
    ACUERDO, HALLAZGO,
    item("Hallazgo en el Amazonas", "https://www.eltiempo.com/vida/ciencia/hallazgo-1"),   # repetida en lote
    item("Sin enlace", None),                                                              # inválida
    item("Fecha rota", "https://www.eltiempo.com/vida/ciencia/x", pub="ayer"),             # inválida
    item("", "https://www.eltiempo.com/vida/ciencia/y"),                                   # inválida
)
FEED_AMBIENTE = feed(
    item("La NASA y Bogotá firman acuerdo", "https://WWW.eltiempo.com/vida/ciencia/acuerdo-123/",
         "&lt;p&gt;Gustavo Petro celebró el &lt;b&gt;acuerdo&lt;/b&gt; con la NASA.&lt;/p&gt;"),
    item("Ceci est un article", "https://www.eltiempo.com/vida/medio-ambiente/fr", "Le climat change.",
         pub="2026-10-21T10:00:00-05:00"),
)


class FakeComprehend:
    ENT = {"Gustavo Petro": "PERSON", "NASA": "ORGANIZATION", "Universidad Nacional": "ORGANIZATION",
           "Bogotá": "LOCATION", "Amazonas": "LOCATION", "lunes": "DATE"}
    KP = ["el acuerdo", "Científicos", "la NASA", "Amazonas"]

    def __init__(self, fail_texts=()):
        self.docs = {"lang": 0, "ent": 0, "kp": 0}
        self.fail_texts = fail_texts

    def batch_detect_dominant_language(self, TextList):
        self.docs["lang"] += len(TextList)
        return {"ResultList": [{"Index": i, "Languages": [
            {"LanguageCode": "fr" if "Ceci" in t else "es", "Score": 0.99}]} for i, t in enumerate(TextList)],
            "ErrorList": []}

    def batch_detect_entities(self, TextList, LanguageCode):
        self.docs["ent"] += len(TextList)
        res, err = [], []
        for i, t in enumerate(TextList):
            if any(f in t for f in self.fail_texts):
                err.append({"Index": i, "ErrorCode": "InternalServerException", "ErrorMessage": "boom"})
                continue
            ents = []
            for name, typ in self.ENT.items():
                start = 0
                while (pos := t.find(name, start)) >= 0:
                    ents.append({"Type": typ, "Text": name, "Score": 0.95, "BeginOffset": pos,
                                 "EndOffset": pos + len(name)})
                    start = pos + 1
            res.append({"Index": i, "Entities": ents})
        return {"ResultList": res, "ErrorList": err}

    def batch_detect_key_phrases(self, TextList, LanguageCode):
        self.docs["kp"] += len(TextList)
        return {"ResultList": [{"Index": i, "KeyPhrases": [
            {"Text": k, "Score": 0.9, "BeginOffset": t.find(k), "EndOffset": t.find(k) + len(k)}
            for k in self.KP if k in t]} for i, t in enumerate(TextList)], "ErrorList": []}


def setup_aws():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    glue = boto3.client("glue", region_name="us-east-1")
    glue.create_database(DatabaseInput={"Name": DB})
    for t in TABLES:
        glue.create_table(DatabaseName=DB, TableInput={
            "Name": t, "TableType": "EXTERNAL_TABLE",
            "PartitionKeys": [{"Name": "pub_date", "Type": "string"}],
            "StorageDescriptor": {"Location": f"s3://{BUCKET}/{TABLES[t][1]}/", "Columns": [],
                                  "InputFormat": "x", "OutputFormat": "y",
                                  "SerdeInfo": {"SerializationLibrary": "z"}}})
    return s3, glue


def ingest(s3, source, content, run_id):
    fetch = lambda url, **kw: FetchResult(url, 200, content, "application/xml", attempts=1)  # noqa: E731
    return BronzeStore(s3, BUCKET).ingest(source, run_id, HttpConfig(), fetcher=fetch)


class Pipeline:
    """Ejecuta los pasos del DAG (sin Airflow) con un Comprehend falso que cuenta llamadas."""

    def __init__(self, s3, glue, client=None, **ccfg):
        self.s3, self.glue = s3, glue
        self.fake = client or FakeComprehend()
        self.enricher = ComprehendEnricher(self.fake, ComprehendConfig(**ccfg))
        self.silver = SilverProcessor(s3, BUCKET, SilverConfig(), self.enricher)
        self.gold = GoldBuilder(s3, BUCKET)
        self.catalog = CatalogSync(glue, s3, BUCKET, DB)

    def run(self, run_id, **kw):
        self.silver.prepare(run_id, **kw)
        enr = self.silver.enrich(run_id)
        s = self.silver.publish(run_id)
        g = self.gold.build(run_id)
        c = self.catalog.sync()
        return {"silver": s, "gold": g, "catalog": c, "enrich": enr}


def pointer(s3, key=POINTER_KEY):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def read_table(s3, glue, table):
    """Lee una tabla como la vería Athena: solo las particiones registradas en Glue."""
    rows = []
    for p in glue.get_partitions(DatabaseName=DB, TableName=table)["Partitions"]:
        prefix = p["StorageDescriptor"]["Location"].split(f"{BUCKET}/", 1)[1]
        for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []):
            for r in S.from_parquet_bytes(s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read()):
                rows.append({**r, "pub_date": p["Values"][0]})
    return rows


def read_prefix(s3, prefix):
    rows = []
    for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []):
        if o["Key"].endswith(".parquet"):
            rows += S.from_parquet_bytes(s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read())
        elif o["Key"].endswith(".json"):
            body = s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read().decode()
            rows += [json.loads(line) for line in body.splitlines() if line.strip()]
    return rows


__all__ = ["GOLD_POINTER"]
