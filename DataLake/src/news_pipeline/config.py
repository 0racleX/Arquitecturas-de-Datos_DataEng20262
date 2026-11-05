"""Carga y validación de la configuración de fuentes RSS."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import yaml

SOURCE_ID_RE = re.compile(r"^[a-z0-9_]+$")


@dataclass(frozen=True)
class Source:
    source_id: str
    medio: str
    seccion: str
    url: str
    enabled: bool = True


@dataclass(frozen=True)
class HttpConfig:
    timeout_seconds: int = 20
    max_retries: int = 3
    backoff_factor: float = 2.0
    user_agent: str = "news-datalake/1.0"


@dataclass(frozen=True)
class SilverConfig:
    lookback_days: int = 7               # ventana de manifiestos Bronze revisados en modo incremental
    future_tolerance_hours: int = 24     # fechas de publicación más allá de esto -> cuarentena
    entity_types: tuple[str, ...] = ("PERSON", "ORGANIZATION", "LOCATION")


@dataclass(frozen=True)
class PipelineConfig:
    sources: list[Source]
    http: HttpConfig
    silver: SilverConfig = field(default_factory=SilverConfig)
    comprehend: "ComprehendConfig" = None  # type: ignore[assignment]

    def enabled_sources(self) -> list[Source]:
        return [s for s in self.sources if s.enabled]

    def get(self, source_id: str) -> Source:
        for s in self.sources:
            if s.source_id == source_id:
                return s
        raise KeyError(f"Fuente no configurada: {source_id}")


def _validate_source(raw: dict) -> Source:
    missing = {"source_id", "medio", "seccion", "url"} - raw.keys()
    if missing:
        raise ValueError(f"Fuente incompleta, faltan {sorted(missing)}: {raw}")
    if not SOURCE_ID_RE.match(raw["source_id"]):
        raise ValueError(f"source_id inválido (use [a-z0-9_]): {raw['source_id']}")
    parsed = urlparse(raw["url"])
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"URL inválida: {raw['url']}")
    # La página del directorio RSS no es un feed: exigimos la ruta a un .xml
    if not parsed.path.endswith(".xml"):
        raise ValueError(f"La URL no parece un feed RSS (.xml): {raw['url']}")
    return Source(
        source_id=raw["source_id"],
        medio=raw["medio"],
        seccion=raw["seccion"],
        url=raw["url"],
        enabled=bool(raw.get("enabled", True)),
    )


def load_config(path: str | Path) -> PipelineConfig:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    sources = [_validate_source(s) for s in data.get("sources", [])]
    ids = [s.source_id for s in sources]
    if len(ids) != len(set(ids)):
        raise ValueError("source_id duplicado en la configuración")
    if not sources:
        raise ValueError("No hay fuentes configuradas")
    http = HttpConfig(**(data.get("http") or {}))
    silver_raw = dict(data.get("silver") or {})
    if "entity_types" in silver_raw:
        silver_raw["entity_types"] = tuple(silver_raw["entity_types"])
    comp_raw = dict(data.get("comprehend") or {})
    if "supported_languages" in comp_raw:
        comp_raw["supported_languages"] = tuple(comp_raw["supported_languages"])
    from .comprehend_enricher import ComprehendConfig
    return PipelineConfig(sources=sources, http=http, silver=SilverConfig(**silver_raw),
                          comprehend=ComprehendConfig(**comp_raw))
