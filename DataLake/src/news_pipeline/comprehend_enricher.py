"""Enriquecimiento con Amazon Comprehend (entidades y frases clave).

Texto analizado = título + descripción limpios. La caché se indexa por
text_hash = sha256(ANALYSIS_VERSION + texto normalizado): si el texto de una
noticia no cambia, su hash tampoco y no se vuelve a llamar a Comprehend.

Estados de procesamiento (nlp_status):
  ANALYZED                       analizado completo
  ANALYZED_TRUNCATED             superaba max_text_bytes; se analizó el inicio (corte en palabra)
  SKIPPED_EMPTY                  texto vacío o con menos de min_text_chars caracteres
  SKIPPED_UNSUPPORTED_LANGUAGE   idioma detectado fuera de supported_languages
  PENDING_QUOTA                  superó max_documents_per_run; se analiza en la siguiente ejecución
  ERROR                          error de Comprehend; NO se guarda en caché, se reintenta luego
Los estados ANALYZED*, SKIPPED_* se guardan en caché (son deterministas para el mismo texto).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .cleaning import normalize_text, sha256_hex

log = logging.getLogger(__name__)

ANALYSIS_VERSION = "v1"
BATCH_SIZE = 25                       # límite de las APIs Batch* de Comprehend
COMPREHEND_LANGS = {"en", "es", "fr", "de", "it", "pt", "ar", "hi", "ja", "ko", "zh", "zh-TW"}

ANALYZED = "ANALYZED"
ANALYZED_TRUNCATED = "ANALYZED_TRUNCATED"
SKIPPED_EMPTY = "SKIPPED_EMPTY"
SKIPPED_UNSUPPORTED = "SKIPPED_UNSUPPORTED_LANGUAGE"
PENDING_QUOTA = "PENDING_QUOTA"
ERROR = "ERROR"
CACHEABLE = {ANALYZED, ANALYZED_TRUNCATED, SKIPPED_EMPTY, SKIPPED_UNSUPPORTED}


@dataclass(frozen=True)
class ComprehendConfig:
    enabled: bool = True
    supported_languages: tuple[str, ...] = ("es", "en")
    default_language: str = "es"
    min_language_score: float = 0.5
    max_text_bytes: int = 4900          # límite del servicio: 5 KB por documento
    min_text_chars: int = 3
    max_documents_per_run: int = 2000   # tope de costo por ejecución


def build_text(title: str | None, description: str | None) -> str:
    title, description = (title or "").strip(), (description or "").strip()
    if title and description:
        sep = " " if title[-1] in ".!?:;…" else ". "
        return normalize_text(f"{title}{sep}{description}") or ""
    return normalize_text(title or description) or ""


def text_hash(text: str) -> str:
    return sha256_hex(f"{ANALYSIS_VERSION}\n{text}")


def truncate_utf8(text: str, max_bytes: int) -> tuple[str, bool]:
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text, False
    cut = raw[:max_bytes].decode("utf-8", errors="ignore")
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut.rstrip(), True


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass
class Analysis:
    text_hash: str
    analyzed_text: str
    text_bytes: int
    truncated: bool
    status: str
    language: str | None = None
    language_score: float | None = None
    language_source: str | None = None      # detected | default
    analyzed_at_utc: str | None = None
    analysis_version: str = ANALYSIS_VERSION
    error: str | None = None
    entities: list[dict] = field(default_factory=list)      # {type,text,score,begin_offset,end_offset}
    key_phrases: list[dict] = field(default_factory=list)   # {text,score,begin_offset,end_offset}


def _chunks(seq, n=BATCH_SIZE):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


class ComprehendEnricher:
    def __init__(self, client, cfg: ComprehendConfig):
        self.client = client
        self.cfg = cfg
        bad = set(cfg.supported_languages) - COMPREHEND_LANGS
        if bad:
            raise ValueError(f"Comprehend no soporta: {bad}")
        self.calls = {"detect_language": 0, "entities": 0, "key_phrases": 0}

    def analyze(self, texts: dict[str, str]) -> dict[str, Analysis]:
        """texts: {text_hash: texto}. Devuelve un Analysis por hash."""
        out: dict[str, Analysis] = {}
        todo: list[Analysis] = []
        for h, text in texts.items():
            analyzed, truncated = truncate_utf8(text, self.cfg.max_text_bytes)
            a = Analysis(text_hash=h, analyzed_text=analyzed, truncated=truncated,
                         text_bytes=len(text.encode("utf-8")), status="")
            out[h] = a
            if len(analyzed.strip()) < self.cfg.min_text_chars:
                a.status, a.analyzed_at_utc = SKIPPED_EMPTY, _now()
            elif len(todo) >= self.cfg.max_documents_per_run:
                a.status = PENDING_QUOTA
            else:
                todo.append(a)
        if not todo:
            return out

        self._detect_language(todo)
        by_lang: dict[str, list[Analysis]] = {}
        for a in todo:
            if a.status == ERROR:
                continue
            if a.language not in self.cfg.supported_languages:
                a.status, a.analyzed_at_utc = SKIPPED_UNSUPPORTED, _now()
                continue
            by_lang.setdefault(a.language, []).append(a)

        for lang, docs in by_lang.items():
            self._entities(lang, docs)
            self._key_phrases(lang, docs)
            for a in docs:
                if a.status != ERROR:
                    a.status = ANALYZED_TRUNCATED if a.truncated else ANALYZED
                    a.analyzed_at_utc = _now()
        return out

    # ------------------------------------------------------------------ llamadas
    def _call(self, fn_name: str, docs: list[Analysis], **kw) -> dict | None:
        try:
            return getattr(self.client, fn_name)(TextList=[d.analyzed_text for d in docs], **kw)
        except Exception as exc:  # throttling persistente, red, etc.
            log.warning("Comprehend %s falló para %d docs: %s", fn_name, len(docs), exc)
            for d in docs:
                d.status, d.error = ERROR, f"{fn_name}: {type(exc).__name__}: {exc}"
            return None

    @staticmethod
    def _mark_errors(resp: dict, docs: list[Analysis], fn_name: str) -> None:
        for e in resp.get("ErrorList", []):
            d = docs[e["Index"]]
            d.status, d.error = ERROR, f"{fn_name}: {e.get('ErrorCode')}: {e.get('ErrorMessage')}"

    def _detect_language(self, docs: list[Analysis]) -> None:
        for chunk in _chunks(docs):
            self.calls["detect_language"] += 1
            resp = self._call("batch_detect_dominant_language", chunk)
            if resp is None:
                continue
            self._mark_errors(resp, chunk, "detect_language")
            for r in resp.get("ResultList", []):
                d = chunk[r["Index"]]
                langs = sorted(r.get("Languages", []), key=lambda x: x["Score"], reverse=True)
                if langs and langs[0]["Score"] >= self.cfg.min_language_score:
                    d.language, d.language_score, d.language_source = (
                        langs[0]["LanguageCode"], round(langs[0]["Score"], 4), "detected")
                else:  # detección poco confiable (textos muy cortos): idioma del feed
                    d.language = self.cfg.default_language
                    d.language_score = round(langs[0]["Score"], 4) if langs else None
                    d.language_source = "default"

    def _entities(self, lang: str, docs: list[Analysis]) -> None:
        for chunk in _chunks([d for d in docs if d.status != ERROR]):
            self.calls["entities"] += 1
            resp = self._call("batch_detect_entities", chunk, LanguageCode=lang)
            if resp is None:
                continue
            self._mark_errors(resp, chunk, "entities")
            for r in resp.get("ResultList", []):
                chunk[r["Index"]].entities = [
                    {"type": e["Type"], "text": e["Text"], "score": round(e["Score"], 4),
                     "begin_offset": e.get("BeginOffset"), "end_offset": e.get("EndOffset")}
                    for e in r.get("Entities", [])]

    def _key_phrases(self, lang: str, docs: list[Analysis]) -> None:
        for chunk in _chunks([d for d in docs if d.status != ERROR]):
            self.calls["key_phrases"] += 1
            resp = self._call("batch_detect_key_phrases", chunk, LanguageCode=lang)
            if resp is None:
                continue
            self._mark_errors(resp, chunk, "key_phrases")
            for r in resp.get("ResultList", []):
                chunk[r["Index"]].key_phrases = [
                    {"text": k["Text"], "score": round(k["Score"], 4),
                     "begin_offset": k.get("BeginOffset"), "end_offset": k.get("EndOffset")}
                    for k in r.get("KeyPhrases", [])]
