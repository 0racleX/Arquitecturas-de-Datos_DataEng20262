"""Limpieza, identificación y validación de noticias (capa Silver, 4.3.1-4.3.4).

Estrategia de identificación (news_id = sha256(clave)[:32]):
  1. guid con forma de URL  -> URL canónica del guid          (id_strategy = guid_url)
  2. guid que no es URL     -> "guid:<medio>:<guid>"          (id_strategy = guid)
  3. sin guid, con enlace   -> URL canónica del enlace        (id_strategy = link)
  4. sin guid ni enlace     -> "hash:<medio>|<título>|<fecha>" (id_strategy = title_date_hash)

URL canónica: https, host en minúsculas, sin puerto por defecto, sin fragmento,
sin parámetros de seguimiento (utm_*, fbclid, gclid...), parámetros restantes
ordenados y sin "/" final. Así la misma nota publicada en varios feeds del medio
produce el mismo news_id.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .rss_parser import parse_pub_date

TRACKING_PARAMS = {"fbclid", "gclid", "dclid", "mc_cid", "mc_eid", "igshid", "ref", "ref_src"}
BLOCK_TAGS = {"p", "br", "div", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6",
              "tr", "td", "th", "blockquote", "figure", "figcaption", "section", "article"}
TAG_RE = re.compile(r"<\s*/?\s*[a-zA-Z!][^>]*>")
CONTROL_RE = re.compile(r"[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f​-‍﻿]")
MIN_PLAUSIBLE_DATE = datetime(2000, 1, 1, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- HTML
class _Stripper(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip += 1
        elif tag in BLOCK_TAGS:
            self.parts.append(" ")

    def handle_startendtag(self, tag, attrs):
        if tag in BLOCK_TAGS:
            self.parts.append(" ")

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self._skip:
            self._skip -= 1
        elif tag in BLOCK_TAGS:
            self.parts.append(" ")

    def handle_data(self, data):
        if not self._skip:
            self.parts.append(data)


def _strip_once(text: str) -> str:
    p = _Stripper()
    p.feed(text)
    p.close()
    return "".join(p.parts)


def normalize_text(text: str | None) -> str | None:
    if text is None:
        return None
    text = unicodedata.normalize("NFC", text).replace(" ", " ")
    text = CONTROL_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def clean_html(text: str | None) -> str | None:
    """Quita etiquetas y decodifica entidades. Repite si el HTML venía doblemente
    escapado (&amp;lt;p&amp;gt; -> <p> -> texto)."""
    if not text:
        return None
    out = text
    for _ in range(3):
        out = _strip_once(out)
        if not TAG_RE.search(out):
            break
    return normalize_text(out)


def fold(text: str) -> str:
    """minúsculas y sin tildes, para comparar textos."""
    nfkd = unicodedata.normalize("NFKD", text.casefold())
    return re.sub(r"\s+", " ", "".join(c for c in nfkd if not unicodedata.combining(c))).strip()


# --------------------------------------------------------------------------- URLs
def is_http_url(value: str | None) -> bool:
    if not value:
        return False
    try:
        p = urlsplit(value.strip())
    except ValueError:
        return False
    return p.scheme in ("http", "https") and bool(p.hostname)


def canonical_url(url: str) -> str:
    p = urlsplit(url.strip())
    host = (p.hostname or "").lower()
    port = p.port
    netloc = host if port in (None, 80, 443) else f"{host}:{port}"
    query = sorted((k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
                   if not k.lower().startswith("utm_") and k.lower() not in TRACKING_PARAMS)
    path = re.sub(r"/{2,}", "/", p.path or "/")
    if len(path) > 1:
        path = path.rstrip("/")
    return urlunsplit(("https", netloc, path, urlencode(query), ""))


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_identity(*, guid: str | None, link: str | None, title: str | None,
                   pub_date_utc: str | None, medio: str) -> tuple[str, str, str]:
    """Devuelve (news_id, id_strategy, identity_key)."""
    guid = (guid or "").strip() or None
    if guid and is_http_url(guid):
        key, strategy = canonical_url(guid), "guid_url"
    elif guid:
        key, strategy = f"guid:{fold(medio)}:{guid}", "guid"
    elif is_http_url(link):
        key, strategy = canonical_url(link), "link"
    else:
        key, strategy = f"hash:{fold(medio)}|{fold(title or '')}|{pub_date_utc or ''}", "title_date_hash"
    return sha256_hex(key)[:32], strategy, key


# --------------------------------------------------------------------------- validación
def validate(*, title: str | None, link: str | None, pub_date_raw: str | None,
             pub_date_utc: str | None, extracted_at_utc: str,
             future_tolerance_hours: int = 24) -> list[str]:
    """Campos obligatorios: título, enlace http(s) y fecha de publicación válida."""
    reasons: list[str] = []
    if not title:
        reasons.append("MISSING_TITLE")
    if not link:
        reasons.append("MISSING_LINK")
    elif not is_http_url(link):
        reasons.append("INVALID_LINK")
    if not pub_date_raw:
        reasons.append("MISSING_PUB_DATE")
    elif not pub_date_utc:
        reasons.append("INVALID_PUB_DATE")
    else:
        pub = datetime.fromisoformat(pub_date_utc.replace("Z", "+00:00"))
        extracted = datetime.fromisoformat(extracted_at_utc.replace("Z", "+00:00"))
        if pub > extracted + timedelta(hours=future_tolerance_hours):
            reasons.append("FUTURE_PUB_DATE")
        elif pub < MIN_PLAUSIBLE_DATE:
            reasons.append("IMPLAUSIBLE_PUB_DATE")
    return reasons


def content_hash(title: str | None, description: str | None, link: str | None,
                 pub_date_utc: str | None) -> str:
    """Huella de los campos que definen la versión de una noticia. `link` debe ser
    la URL canónica: variantes de tracking (utm_*, "/" final) no son un cambio."""
    return sha256_hex("␟".join(x or "" for x in (title, description, link, pub_date_utc)))


CHANGE_FIELDS = ("title", "description", "link_canonical", "pub_date_utc")


def changed_fields(old: dict, new: dict) -> list[str]:
    return [f for f in CHANGE_FIELDS if (old.get(f) or None) != (new.get(f) or None)]


__all__ = ["clean_html", "normalize_text", "fold", "canonical_url", "is_http_url",
           "build_identity", "validate", "content_hash", "changed_fields", "parse_pub_date",
           "sha256_hex", "CHANGE_FIELDS"]
