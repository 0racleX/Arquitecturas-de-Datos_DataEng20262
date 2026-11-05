"""Descarga de feeds con reintentos. Nunca lanza por errores HTTP: devuelve
un FetchResult para que el lote quede registrado aunque falle (punto 4.1.5)."""
from __future__ import annotations

import time
from dataclasses import dataclass, field

import requests

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass
class FetchResult:
    url: str
    http_status: int | None          # None si no hubo respuesta (timeout, DNS...)
    content: bytes = b""
    content_type: str | None = None
    attempts: int = 0
    error: str | None = None
    elapsed_ms: int = 0
    headers: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.http_status == 200 and self.error is None


def fetch(url: str, *, timeout: int = 20, max_retries: int = 3,
          backoff_factor: float = 2.0, user_agent: str = "news-datalake/1.0",
          session: requests.Session | None = None, sleep=time.sleep) -> FetchResult:
    sess = session or requests.Session()
    headers = {"User-Agent": user_agent,
               "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.5"}
    result = FetchResult(url=url, http_status=None)
    start = time.monotonic()

    for attempt in range(1, max_retries + 2):          # 1 intento + max_retries
        result.attempts = attempt
        try:
            resp = sess.get(url, headers=headers, timeout=timeout)
            result.http_status = resp.status_code
            result.content = resp.content or b""
            result.content_type = resp.headers.get("Content-Type")
            result.headers = {k: v for k, v in resp.headers.items()
                              if k.lower() in ("etag", "last-modified", "content-type", "content-length")}
            result.error = None if resp.status_code == 200 else f"HTTP {resp.status_code}"
            if resp.status_code not in RETRYABLE_STATUS:
                break
        except requests.RequestException as exc:
            result.http_status = None
            result.content = b""
            result.error = f"{type(exc).__name__}: {exc}"
        if attempt <= max_retries:
            sleep(backoff_factor ** attempt)

    result.elapsed_ms = int((time.monotonic() - start) * 1000)
    return result
