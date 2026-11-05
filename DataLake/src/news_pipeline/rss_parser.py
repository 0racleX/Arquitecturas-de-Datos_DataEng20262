"""Parseo de feeds RSS 2.0 (con soporte básico de Atom).

Extrae: identificador, título, enlace, descripción y fecha de publicación,
y añade la fuente y la fecha de extracción (punto 4.1.3 y 4.1.4).
"""
from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from xml.etree.ElementTree import ParseError

from defusedxml import ElementTree as SafeET  # protege contra XXE / billion laughs

ATOM_NS = "{http://www.w3.org/2005/Atom}"


class MalformedFeedError(Exception):
    """El contenido no es XML válido o no es un feed RSS/Atom reconocible."""


class EmptyFeedError(Exception):
    """La respuesta está vacía o el feed no contiene ítems."""


@dataclass
class NewsItem:
    item_id: str
    guid: str | None
    title: str | None
    link: str | None
    description: str | None
    pub_date_raw: str | None
    pub_date_utc: str | None      # ISO 8601 en UTC, None si no se pudo interpretar
    source_id: str
    medio: str
    seccion: str
    source_url: str
    extracted_at_utc: str
    run_id: str

    def to_dict(self) -> dict:
        return asdict(self)


def parse_pub_date(value: str | None) -> str | None:
    """El Tiempo usa ISO 8601 en los ítems (2026-10-20T09:54:20-05:00) y
    RFC 822 en el canal (Wed, 21 Oct 2026 07:23:35 -0500). Aceptamos ambos."""
    if not value:
        return None
    value = value.strip()
    dt = None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:          # sin zona: asumimos UTC y lo dejamos explícito
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _text(el, tag: str) -> str | None:
    child = el.find(tag)
    if child is None or child.text is None:
        return None
    txt = child.text.strip()
    return txt or None


def _make_id(guid: str | None, link: str | None, title: str | None, pub: str | None) -> str:
    """guid > link > hash(título+fecha). Siempre devuelve un id estable."""
    if guid:
        return guid
    if link:
        return link
    basis = f"{title or ''}|{pub or ''}".encode("utf-8")
    return "sha256:" + hashlib.sha256(basis).hexdigest()


def parse_feed(content: bytes, *, source_id: str, medio: str, seccion: str,
               source_url: str, extracted_at_utc: str, run_id: str) -> list[NewsItem]:
    if not content or not content.strip():
        raise EmptyFeedError("Respuesta vacía")
    try:
        root = SafeET.fromstring(content)
    except ParseError as exc:
        raise MalformedFeedError(f"XML malformado: {exc}") from exc
    except Exception as exc:  # defusedxml lanza sus propias excepciones (EntitiesForbidden, ...)
        raise MalformedFeedError(f"XML rechazado: {exc}") from exc

    common = dict(source_id=source_id, medio=medio, seccion=seccion,
                  source_url=source_url, extracted_at_utc=extracted_at_utc, run_id=run_id)
    items: list[NewsItem] = []

    if root.tag == "rss":
        channel = root.find("channel")
        if channel is None:
            raise MalformedFeedError("RSS sin <channel>")
        for it in channel.findall("item"):
            guid, link = _text(it, "guid"), _text(it, "link")
            title, pub = _text(it, "title"), _text(it, "pubDate")
            items.append(NewsItem(
                item_id=_make_id(guid, link, title, pub), guid=guid, title=title, link=link,
                description=_text(it, "description"), pub_date_raw=pub,
                pub_date_utc=parse_pub_date(pub), **common))
    elif root.tag == f"{ATOM_NS}feed":
        for entry in root.findall(f"{ATOM_NS}entry"):
            link_el = entry.find(f"{ATOM_NS}link")
            link = link_el.get("href") if link_el is not None else None
            guid = _text(entry, f"{ATOM_NS}id")
            title = _text(entry, f"{ATOM_NS}title")
            pub = _text(entry, f"{ATOM_NS}published") or _text(entry, f"{ATOM_NS}updated")
            items.append(NewsItem(
                item_id=_make_id(guid, link, title, pub), guid=guid, title=title, link=link,
                description=_text(entry, f"{ATOM_NS}summary"), pub_date_raw=pub,
                pub_date_utc=parse_pub_date(pub), **common))
    else:
        raise MalformedFeedError(f"Raíz no reconocida como RSS/Atom: <{root.tag}>")

    if not items:
        raise EmptyFeedError("El feed no contiene ítems")
    return items
