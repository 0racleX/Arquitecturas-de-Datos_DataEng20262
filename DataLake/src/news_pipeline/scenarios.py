"""Lotes controlados para demostrar los escenarios de calidad (4.5).

Toma el último RSS real guardado en Bronze para una fuente, lo modifica y lo
guarda como un lote Bronze NUEVO (mode=controlled_copy). Nunca toca el original.

  texto       cambia el título de la primera noticia  -> nueva versión + nuevo análisis Comprehend
  fecha       cambia solo la fecha de la primera      -> nueva versión SIN llamar a Comprehend
  defectuoso  agrega 4 registros inválidos y 1 repetido -> cuarentena con motivo + duplicado detectado

Uso (en la EC2):
  python -m news_pipeline.scenarios --bucket $DATALAKE_BUCKET --source eltiempo_ciencia --scenario texto
Luego dispare el DAG en modo reprocess con el run_id que imprime.
"""
from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime

ITEM_RE = re.compile(r"<item\b.*?</item>", re.S)
TITLE_RE = re.compile(r"(<title>)(.*?)(</title>)", re.S)
PUB_RE = re.compile(r"(<pubDate>)(.*?)(</pubDate>)", re.S)
SUFFIX = " (actualizado)"


def _first_item(xml: str) -> re.Match:
    m = ITEM_RE.search(xml)
    if not m:
        raise ValueError("El RSS no tiene ítems")
    return m


def modify_title(xml: str) -> tuple[str, str]:
    item = _first_item(xml)
    block = item.group(0)

    def repl(t: re.Match) -> str:
        inner = t.group(2)
        if inner.rstrip().endswith("]]>"):
            inner = inner.rstrip()[:-3] + SUFFIX + "]]>"
        else:
            inner = inner + SUFFIX
        return t.group(1) + inner + t.group(3)

    new_block, n = TITLE_RE.subn(repl, block, count=1)
    if not n:
        raise ValueError("La primera noticia no tiene <title>")
    return xml[:item.start()] + new_block + xml[item.end():], TITLE_RE.search(new_block).group(2)


def shift_pub_date(xml: str, hours: int = 1) -> tuple[str, str]:
    item = _first_item(xml)
    block = item.group(0)
    m = PUB_RE.search(block)
    if not m:
        raise ValueError("La primera noticia no tiene <pubDate>")
    raw = m.group(2).strip()
    try:
        new = (datetime.fromisoformat(raw.replace("Z", "+00:00")) - timedelta(hours=hours)).isoformat()
    except ValueError:
        new = format_datetime(parsedate_to_datetime(raw) - timedelta(hours=hours))
    new_block = block[:m.start(2)] + new + block[m.end(2):]
    return xml[:item.start()] + new_block + xml[item.end():], new


def add_defective(xml: str) -> tuple[str, list[str]]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    first = _first_item(xml).group(0)
    bad = [
        f"<item><title>Prueba controlada: noticia sin enlace</title><pubDate>{now}</pubDate>"
        f"<description>Registro defectuoso a propósito</description></item>",
        "<item><title>Prueba controlada: fecha inválida</title>"
        "<link>https://www.eltiempo.com/prueba-controlada/fecha-invalida</link>"
        "<pubDate>fecha-no-valida</pubDate><description>Registro defectuoso</description></item>",
        f"<item><title></title><link>https://www.eltiempo.com/prueba-controlada/sin-titulo</link>"
        f"<pubDate>{now}</pubDate><description>Registro defectuoso</description></item>",
        f"<item><title>Prueba controlada: enlace inválido</title><link>ftp://eltiempo.com/x</link>"
        f"<pubDate>{now}</pubDate><description>Registro defectuoso</description></item>",
        first,   # repetida dentro del mismo lote
    ]
    idx = xml.rfind("</channel>")
    if idx < 0:
        raise ValueError("RSS sin </channel>")
    expected = ["MISSING_LINK", "INVALID_PUB_DATE", "MISSING_TITLE", "INVALID_LINK", "DUPLICATE_IN_BATCH"]
    return xml[:idx] + "".join(bad) + xml[idx:], expected


def build(content: bytes, scenario: str) -> tuple[bytes, str]:
    xml = content.decode("utf-8")
    if scenario == "texto":
        out, detail = modify_title(xml)
        note = f"Título de la primera noticia modificado: {detail[:120]}"
    elif scenario == "fecha":
        out, detail = shift_pub_date(xml)
        note = f"Fecha de la primera noticia cambiada a {detail} (texto igual)"
    elif scenario == "defectuoso":
        out, expected = add_defective(xml)
        note = "Agregados registros defectuosos: " + ", ".join(expected)
    elif scenario == "nueva_particion":
        out, detail = shift_pub_date(xml, hours=24 * 30)
        note = f"Fecha de la primera noticia movida 30 días atrás ({detail}) para crear una partición nueva"
    else:
        raise ValueError(f"Escenario desconocido: {scenario}")
    return out.encode("utf-8"), note


def main() -> int:
    import boto3

    from .bronze import BronzeStore
    from .config import load_config

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", required=True)
    p.add_argument("--config", default="config/sources.yaml")
    p.add_argument("--source", required=True)
    p.add_argument("--scenario", required=True, choices=["texto", "fecha", "defectuoso"])
    p.add_argument("--scenario", required=True, choices=["texto", "fecha", "defectuoso", "nueva_particion"])
    a = p.parse_args()

    cfg = load_config(a.config)
    store = BronzeStore(boto3.client("s3"), a.bucket)
    src = cfg.get(a.source)
    base = store.latest_success(src.source_id)
    content, note = build(store.read_bronze(base), a.scenario)
    run_id = f"controlled__{a.scenario}__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    m = store.store_controlled(src, run_id, content, base.run_id, note)
    print(f"Lote controlado creado: run_id={m.run_id}")
    print(f"  basado en: {base.run_id}  ({base.s3_uri})")
    print(f"  nuevo:     {m.s3_uri}")
    print(f"  cambio:    {note}")
    print("\nProcéselo desde Airflow (sin consultar los RSS):")
    print(f"  airflow dags trigger news_datalake_pipeline --conf "
          f"'{{\"mode\": \"reprocess\", \"reprocess_run_id\": \"{m.run_id}\"}}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
