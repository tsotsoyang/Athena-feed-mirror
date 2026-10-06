"""Feed mirror runner.

Fetches every target defined in ``configs/feed_mirror_targets.yaml`` and
writes the result as JSON under ``feed_mirror/{source}.json``. The
written JSON is the stable contract consumed by Athena's
``fetch_mirrored_feed`` primitive — see ``README.md`` for the shape.

Design notes
------------

- **No HV-agent dependency.** This runner stays standalone so it can be
  cloned by anyone re-running the mirror cycle, and so it doesn't have
  to bump alongside Athena every time hv-primitives moves.
- **One target per JSON file** for atomic publishing. If a target fails
  mid-run, only its file is affected.
- **Per-target error isolation** mirrors the rss_blog v2 fix on HV-agent
  (#58): one slow / failing target never poisons the others.
- **Autodiscovery fallback** when feedparser returns 0 entries — same
  pattern as rss_blog v2: fetch URL as HTML, parse
  ``<link rel="alternate" type="application/(rss|atom)+xml">``, retry.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any, NamedTuple
from urllib import robotparser
from urllib.parse import urljoin, urlparse

import yaml

try:  # optional, helps on corp-MITM dev boxes; no-op on GH Actions
    import truststore

    truststore.inject_into_ssl()
except Exception:  # noqa: BLE001
    pass

import feedparser  # noqa: E402

USER_AGENT = (
    "Mozilla/5.0 (compatible; Athena-feed-mirror/0.1; "
    "+https://github.com/tsotsoyang/Athena-feed-mirror)"
)
feedparser.USER_AGENT = USER_AGENT

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGETS_FILE = REPO_ROOT / "configs" / "feed_mirror_targets.yaml"
OUTPUT_DIR = REPO_ROOT / "feed_mirror"
CRON_INTERVAL = timedelta(hours=4)  # keep in step with the workflow cron

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _clean(text: str) -> str:
    text = _HTML_TAG_RE.sub(" ", text or "")
    return _WS_RE.sub(" ", text).strip()


def _parse_date(entry: Any) -> str | None:
    """Return ISO-8601 string in UTC or None."""
    for attr in ("published", "updated"):
        val = entry.get(attr) if isinstance(entry, dict) else getattr(entry, attr, None)
        if not val:
            continue
        try:
            dt = parsedate_to_datetime(val)
        except Exception:  # noqa: BLE001
            try:
                dt = datetime.strptime(val[:10], "%Y-%m-%d")
            except Exception:  # noqa: BLE001
                continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    return None


def _autodiscover_feed_urls(html: str, base_url: str) -> list[str]:
    """Parse <link rel="alternate" type="application/(rss|atom)+xml"> from HTML."""
    if not html:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"<link\b[^>]*>", html, flags=re.IGNORECASE):
        tag = match.group(0)
        if not re.search(r'rel\s*=\s*["\']?\s*alternate', tag, flags=re.IGNORECASE):
            continue
        if not re.search(
            r'type\s*=\s*["\']?\s*application/(rss|atom)\+xml',
            tag,
            flags=re.IGNORECASE,
        ):
            continue
        href_match = re.search(r'href\s*=\s*["\']([^"\']+)["\']', tag, flags=re.IGNORECASE)
        if not href_match:
            continue
        href = href_match.group(1).strip()
        if not href:
            continue
        absolute = urljoin(base_url, href) if not href.startswith("http") else href
        if absolute in seen:
            continue
        seen.add(absolute)
        out.append(absolute)
    return out


# Browser UA for HTML fetches (autodiscovery + scrape). CDN/WAF-fronted
# sites (Cloudflare on x.ai, ai.meta.com) reject obvious bot UAs — same
# gotcha as Athena's source_health false-dead fix. Feed fetches keep the
# honest USER_AGENT above.
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _fetch_html(url: str) -> str:
    """Best-effort HTML fetch for autodiscovery; returns '' on any failure."""
    try:
        import httpx
    except ImportError:
        return ""
    try:
        resp = httpx.get(
            url,
            headers={"User-Agent": BROWSER_UA, "Accept": "text/html,*/*"},
            follow_redirects=True,
            timeout=20.0,
        )
    except Exception:  # noqa: BLE001
        return ""
    if resp.status_code >= 400:
        return ""
    return resp.text[:500_000]


def _parse_with_autodiscovery(feed_url: str) -> tuple[Any, str, list[dict]]:
    """Try feed_url; on 0 entries, autodiscover from the HTML once.

    Returns ``(parsed_feed, effective_url, errors)`` so callers can
    record which URL actually delivered data.
    """
    errors: list[dict] = []
    parsed = feedparser.parse(feed_url, agent=USER_AGENT)
    if getattr(parsed, "entries", None):
        return parsed, feed_url, errors
    if not urlparse(feed_url).netloc:
        return parsed, feed_url, errors
    html = _fetch_html(feed_url)
    discovered = _autodiscover_feed_urls(html, feed_url)
    if not discovered:
        errors.append({"stage": "autodiscovery", "message": "no alternate links found"})
        return parsed, feed_url, errors
    for href in discovered:
        retry = feedparser.parse(href, agent=USER_AGENT)
        if getattr(retry, "entries", None):
            return retry, href, errors
    errors.append({"stage": "autodiscovery", "message": "discovered URLs returned 0 entries"})
    return parsed, feed_url, errors


def _meta_content(html: str, key: str) -> str:
    """Extract <meta name|property=key content=...> regardless of attr order."""
    for match in re.finditer(r"<meta\b[^>]*>", html, flags=re.IGNORECASE):
        tag = match.group(0)
        if not re.search(
            rf'(?:name|property)\s*=\s*["\']{re.escape(key)}["\']', tag, flags=re.IGNORECASE
        ):
            continue
        content = re.search(r'content\s*=\s*["\']([^"\']+)["\']', tag, flags=re.IGNORECASE)
        if content:
            return _clean(content.group(1))
    return ""


def _scrape_index(target: dict) -> tuple[list[dict], list[dict], str]:
    """HTML index-scrape fallback for sites with no feed at all (opt-in).

    Target config:
        scrape:
          index_url: https://www.kimi.com/blog   # page listing the posts
          link_prefix: /blog/                     # keep only anchors under this path
          enrich: true                            # fetch each post for title/description

    Returns ``(entries, errors, index_url)``. Anchors are same-host only,
    deduped, the index page itself excluded. With ``enrich`` (default on)
    each kept post page is fetched once for its <title> + description meta —
    that's what gives Athena's keyword scoring something to bite on.
    """
    cfg = target.get("scrape") or {}
    index_url = str(cfg.get("index_url") or target["url"])
    link_prefix = str(cfg.get("link_prefix") or "")
    max_entries = int(target.get("max_entries", 25))
    enrich = bool(cfg.get("enrich", True))
    # canonical_host: when the index is fetched through a proxy host (e.g.
    # x-ai.translate.goog because the WAF rejects direct datacenter
    # fetches), emitted entry URLs are rewritten to this host so Athena
    # stores the CANONICAL address; enrichment still fetches the proxy URL.
    canonical_host = str(cfg.get("canonical_host") or "")
    if not link_prefix:
        return [], [{"stage": "scrape", "message": "scrape.link_prefix not set"}], index_url

    html_text = _fetch_html(index_url)
    if not html_text:
        return (
            [],
            [{"stage": "scrape", "message": f"index fetch failed: {index_url}"}],
            index_url,
        )

    index_host = urlparse(index_url).netloc
    index_path = urlparse(index_url).path.rstrip("/")
    seen: set[str] = set()
    links: list[tuple[str, str, str]] = []  # (canonical, fetch_url, anchor_text)
    for match in re.finditer(
        r"<a\b[^>]*href\s*=\s*[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        absolute = urljoin(index_url, unescape(match.group(1)))
        parsed = urlparse(absolute)
        if parsed.netloc != index_host:
            continue
        path = parsed.path
        if not path.startswith(link_prefix):
            continue
        if path.rstrip("/") == index_path:
            continue
        emit_host = canonical_host or parsed.netloc
        canonical = f"https://{emit_host}{path.rstrip('/')}"
        if canonical in seen:
            continue
        seen.add(canonical)
        links.append((canonical, absolute, _clean(match.group(2))))

    entries: list[dict] = []
    for url, fetch_url, anchor_text in links[:max_entries]:
        title = anchor_text
        summary = ""
        if enrich:
            page = _fetch_html(fetch_url)
            if page:
                title_match = re.search(
                    r"<title[^>]*>(.*?)</title>", page, flags=re.IGNORECASE | re.DOTALL
                )
                if title_match and _clean(title_match.group(1)):
                    title = _clean(title_match.group(1))
                summary = (
                    _meta_content(page, "description")
                    or _meta_content(page, "og:description")
                )[:2000]
        if not title:
            title = url.rstrip("/").rsplit("/", 1)[-1].replace("-", " ").title()
        entries.append(
            {
                "id": url,
                "title": title,
                "url": url,
                "published_at": None,
                "summary": summary,
                "author": None,
            }
        )
    errors: list[dict] = []
    if not entries:
        errors.append({"stage": "scrape", "message": "no matching anchors on index page"})
    return entries, errors, index_url


def _entry_to_dict(entry: Any) -> dict[str, Any]:
    title = entry.get("title", "") or ""
    raw_summary = entry.get("summary", "") or ""
    if not raw_summary:
        contents = entry.get("content") or [{}]
        if contents:
            raw_summary = contents[0].get("value", "") or ""
    summary = _clean(raw_summary)[:2000]
    link = entry.get("link", "") or ""
    author = entry.get("author", "") or ""
    link = link.strip()
    return {
        "id": (str(entry.get("id", "") or "").strip() or link),
        "title": title.strip(),
        "url": link,
        "published_at": _parse_date(entry),
        "summary": summary,
        "author": author.strip() or None,
    }


def _fetch_target(target: dict) -> dict:
    """Fetch one target end-to-end and return the JSON payload."""
    name = target["name"]
    source = target["source"]
    max_entries = int(target.get("max_entries", 25))

    urls_to_try: list[str] = [target["url"]] + list(target.get("fallback_urls") or [])
    errors: list[dict] = []
    parsed = None
    effective_url = urls_to_try[0]

    for candidate in urls_to_try:
        parsed, effective_url, autodisco_errors = _parse_with_autodiscovery(candidate)
        errors.extend(autodisco_errors)
        if getattr(parsed, "entries", None):
            break

    entries: list[dict] = []
    if parsed and getattr(parsed, "entries", None):
        for raw in parsed.entries[:max_entries]:
            d = _entry_to_dict(raw)
            if d["title"] and d["url"]:
                entries.append(d)
    elif target.get("scrape"):
        # No feed anywhere — fall back to scraping the HTML index (opt-in).
        entries, scrape_errors, effective_url = _scrape_index(target)
        errors.extend(scrape_errors)
    else:
        errors.append({"stage": "fetch", "message": "all candidate URLs returned 0 entries"})

    now = datetime.now(timezone.utc)
    return {
        "source": source,
        "name": name,
        "feed_url": effective_url,
        "fetched_at": now.isoformat(),
        "next_refresh_at": (now + CRON_INTERVAL).isoformat(),
        "entries": entries,
        "errors": errors,
    }


# ── Article text (KP-R11, 2026-10) ───────────────────────────────────────────
# Content is a BEST-EFFORT second phase: every feed JSON is written (with
# whatever article text is already known) BEFORE the first article is fetched,
# so a slow site, a hung extraction or a killed process can cost article text
# but never the feed update itself. Every knob below bounds either wall-clock
# (the whole content phase is CONTENT_RUN_BUDGET_S) or growth of the committed
# files (each file is capped, the state file is pruned to live entries).
CONTENT_MAX_CHARS = 32_000  # == hv_primitives FULLTEXT_TEXT_MAX_CHARS (the consumer truncates here)
CONTENT_MIN_CHARS = 200  # shorter than this is a paywall/teaser/stub, not an article
CONTENT_FETCH_TIMEOUT_S = 15.0  # TOTAL wall-clock per article (not per socket read)
CONTENT_RUN_BUDGET_S = 120.0  # whole content phase, all articles
CONTENT_MAX_NEW_PER_RUN = 60  # fetch ATTEMPTS per run (successes and failures)
CONTENT_MAX_ATTEMPTS = 3  # per entry, ever; permanent failures use them up at once
CONTENT_MAX_BYTES = 2_000_000  # bytes read from one article response
CONTENT_HOST_SPACING_S = 2.0  # minimum gap between two requests to one host
CONTENT_MAX_REDIRECTS = 5
MAX_PAYLOAD_BYTES = 1_500_000  # one {source}.json; oldest entries lose content first
MAX_STATE_KEYS_PER_SOURCE = 500
STATE_SUBDIR = "_state"
CONTENT_STATE_FILE = "content_state.json"
# Chrome-shaped (WAF-fronted publishers reject bare bot UAs) but with an
# identifying product token so a publisher can see who is asking.
CONTENT_USER_AGENT = (
    f"{BROWSER_UA} Athena-feed-mirror/0.2 (+https://github.com/tsotsoyang/Athena-feed-mirror)"
)
ROBOTS_TOKEN = "Athena-feed-mirror"

_clock = time.monotonic
_sleep = time.sleep
# Hosts allowed despite resolving to a non-public address. Empty in
# production; the tests add 127.0.0.1 for their local server.
_ALLOWED_PRIVATE_HOSTS: set[str] = set()
_ROBOTS: dict[str, robotparser.RobotFileParser | None] = {}
_CLIENT: Any = None


def _http() -> Any:
    """One shared client for the content phase (main thread only). Building
    an httpx client loads the CA bundle - about 4 s on a Defender-scanned
    Windows box - so it is created once per run, not once per request."""
    global _CLIENT
    if _CLIENT is None:
        import httpx

        _CLIENT = httpx.Client(follow_redirects=False)
    return _CLIENT


def _close_http() -> None:
    global _CLIENT
    client, _CLIENT = _CLIENT, None
    if client is not None:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass

# A failure that retrying cannot fix: it uses the entry's attempts up at once.
_PERMANENT_REASONS = frozenset(
    {
        "http_forbidden",
        "http_gone",
        "http_client_error",
        "not_html",
        "robots",
        "blocked_url",
        "blocked_host",
        "too_short",
    }
)
_STATE_STATUSES = frozenset({"ok", "empty", "dropped"})


class ArticleResult(NamedTuple):
    text: str  # '' unless reason == "ok"
    reason: str  # "ok" or a short failure label


class ContentBudget:
    """Per-run cap on article fetch ATTEMPTS: a wall-clock deadline and a
    count. Used from the main thread only, so it needs no lock."""

    def __init__(self, seconds: float, max_new: int, *, clock=None) -> None:
        self._clock = clock or _clock
        self._deadline = self._clock() + max(0.0, float(seconds))
        self._left = max(0, int(max_new))
        self.exhausted = False

    def remaining(self) -> float:
        return max(0.0, self._deadline - self._clock())

    def has_room(self) -> bool:
        if self._left <= 0 or self.remaining() <= 0:
            self.exhausted = True
            return False
        return True

    def take(self) -> bool:
        if not self.has_room():
            return False
        self._left -= 1
        return True


def _entry_key(entry: dict) -> str:
    """Stable identity of an entry: feed guid, else URL. A very long id is
    hashed so the state file's size stays bounded."""
    key = str(entry.get("id") or entry.get("url") or "").strip()
    if len(key) > 300:
        key = "sha1:" + hashlib.sha1(key.encode("utf-8")).hexdigest()
    return key


def _url_block_reason(url: str) -> str | None:
    """Why this URL must not be fetched, or None. The runner follows links
    taken from third-party feeds from inside GitHub's network, so only
    http(s) to public addresses is allowed (no loopback / link-local /
    private ranges, e.g. cloud metadata endpoints). Resolution happens again
    inside the HTTP stack; this is a guard against hostile links, not a
    defence against DNS rebinding."""
    parts = urlparse(url)
    host = (parts.hostname or "").lower()
    if parts.scheme not in ("http", "https") or not host:
        return "blocked_url"
    if host in _ALLOWED_PRIVATE_HOSTS:
        return None
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return "network"
    for info in infos:
        try:
            ip = ipaddress.ip_address(str(info[4][0]).split("%")[0])
        except ValueError:
            return "blocked_host"
        if not ip.is_global:
            return "blocked_host"
    return None if infos else "network"


def _robots_allows(url: str, *, timeout_s: float) -> bool:
    """robots.txt for the URL's host (cached per run). Fail-open: an
    unreachable or unparseable robots.txt does not forbid the fetch."""
    parts = urlparse(url)
    origin = f"{parts.scheme}://{parts.netloc}"
    if origin not in _ROBOTS:
        parsed: robotparser.RobotFileParser | None = None
        try:
            resp = _http().get(
                f"{origin}/robots.txt",
                headers={"User-Agent": CONTENT_USER_AGENT},
                timeout=min(5.0, timeout_s),
            )
            if resp.status_code == 200:
                parsed = robotparser.RobotFileParser()
                parsed.parse(resp.text[:500_000].splitlines())
        except Exception:  # noqa: BLE001
            parsed = None
        _ROBOTS[origin] = parsed
    rules = _ROBOTS[origin]
    return True if rules is None else rules.can_fetch(ROBOTS_TOKEN, url)


def _status_reason(code: int) -> str | None:
    if code < 400:
        return None
    if code in (401, 402, 403, 451):
        return "http_forbidden"
    if code in (404, 410):
        return "http_gone"
    if code in (408, 425, 429) or code >= 500:
        return "http_retry"
    return "http_client_error"


def _normalize_text(text: str) -> str:
    text = text.replace("\x00", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _finalize_content(text: str) -> str:
    """The output contract for ``content``: a stripped string of at most
    CONTENT_MAX_CHARS. Applied to fresh and to carried-over text alike."""
    return text.strip()[:CONTENT_MAX_CHARS].rstrip()


def _fetch_article(url: str, *, timeout_s: float = CONTENT_FETCH_TIMEOUT_S) -> ArticleResult:
    """Article body as plain text via trafilatura. Never raises: any failure
    is an ArticleResult('', <reason>). ``timeout_s`` bounds the WHOLE item
    (connect + redirects + body), and the body is read at most
    CONTENT_MAX_BYTES, so a slow-drip or endless response cannot stall a run."""
    try:
        import httpx
        import trafilatura
    except ImportError:
        return ArticleResult("", "dependency_missing")

    deadline = _clock() + max(0.1, timeout_s)
    body = bytearray()
    current = url
    try:
        for _hop in range(CONTENT_MAX_REDIRECTS + 1):
            blocked = _url_block_reason(current)
            if blocked:
                return ArticleResult("", blocked)
            remaining = deadline - _clock()
            if remaining <= 0:
                return ArticleResult("", "timeout")
            if not _robots_allows(current, timeout_s=remaining):
                return ArticleResult("", "robots")
            remaining = deadline - _clock()
            if remaining <= 0:
                return ArticleResult("", "timeout")
            with _http().stream(
                "GET",
                current,
                headers={"User-Agent": CONTENT_USER_AGENT, "Accept": "text/html,*/*"},
                timeout=remaining,
            ) as resp:
                if resp.status_code in (301, 302, 303, 307, 308):
                    location = resp.headers.get("location")
                    if not location:
                        return ArticleResult("", "http_client_error")
                    current = urljoin(current, location)
                    continue
                reason = _status_reason(resp.status_code)
                if reason:
                    return ArticleResult("", reason)
                ctype = resp.headers.get("content-type", "").lower()
                if ctype and "html" not in ctype:
                    return ArticleResult("", "not_html")
                for chunk in resp.iter_bytes():
                    body.extend(chunk)
                    if len(body) >= CONTENT_MAX_BYTES:
                        del body[CONTENT_MAX_BYTES:]
                        break
                    if _clock() > deadline:
                        return ArticleResult("", "timeout")
            break
        else:
            return ArticleResult("", "http_client_error")  # redirect loop
    except httpx.TimeoutException:
        return ArticleResult("", "timeout")
    except Exception:  # noqa: BLE001
        return ArticleResult("", "network")

    try:
        text = trafilatura.extract(bytes(body), include_comments=False, include_tables=True) or ""
    except Exception:  # noqa: BLE001
        return ArticleResult("", "extract_error")
    text = _normalize_text(text)
    if not text:
        return ArticleResult("", "empty")
    if len(text) < CONTENT_MIN_CHARS:
        return ArticleResult("", "too_short")
    return ArticleResult(_finalize_content(text), "ok")


def _load_prior_content(source: str, output_dir: Path) -> dict[str, str]:
    """Article text already published for this source, keyed by entry key —
    the cache that makes fetching incremental. The JSON file itself is the
    source of truth; the state file only counts attempts."""
    try:
        payload = json.loads((output_dir / f"{source}.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out: dict[str, str] = {}
    entries = payload.get("entries") if isinstance(payload, dict) else None
    for e in entries or []:
        if isinstance(e, dict) and isinstance(e.get("content"), str) and e["content"].strip():
            key = _entry_key(e)
            if key:
                out[key] = e["content"]
    return out


def _state_path(output_dir: Path) -> Path:
    return output_dir / STATE_SUBDIR / CONTENT_STATE_FILE


def _sanitize_state(raw: Any) -> dict[str, dict[str, dict]]:
    """Keep only well-formed records; a corrupt state file costs a retry,
    never a crash."""
    out: dict[str, dict[str, dict]] = {}
    if not isinstance(raw, dict):
        return out
    for source, recs in raw.items():
        if not isinstance(source, str) or not isinstance(recs, dict):
            continue
        clean: dict[str, dict] = {}
        for key, rec in list(recs.items())[:MAX_STATE_KEYS_PER_SOURCE]:
            if not isinstance(key, str) or not isinstance(rec, dict):
                continue
            status = rec.get("status")
            attempts = rec.get("attempts")
            if status not in _STATE_STATUSES or not isinstance(attempts, int):
                continue
            item: dict[str, Any] = {"status": status, "attempts": max(0, min(attempts, 100))}
            if isinstance(rec.get("reason"), str):
                item["reason"] = rec["reason"][:40]
            clean[key] = item
        out[source] = clean
    return out


def _load_state(output_dir: Path) -> dict[str, dict[str, dict]]:
    try:
        raw = json.loads(_state_path(output_dir).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return _sanitize_state(raw)


def _atomic_write_text(path: Path, text: str) -> None:
    """Write via a sibling temp file + rename, so a process killed mid-write
    (the step timeout) leaves the previous file intact, never half of one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass


def _write_state(state: dict, output_dir: Path) -> Path:
    path = _state_path(output_dir)
    _atomic_write_text(path, json.dumps(state, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
    return path


def _dump_payload(payload: dict) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False) + "\n"


def _payload_bytes(payload: dict) -> int:
    return len(_dump_payload(payload).encode("utf-8"))


def _cap_payload_bytes(payload: dict, max_bytes: int | None = None) -> list[str]:
    """Drop ``content`` from the OLDEST entries (feeds list newest first)
    until the serialised file fits. Returns the keys whose content was
    dropped, so history grows by new articles instead of by refetches."""
    limit = MAX_PAYLOAD_BYTES if max_bytes is None else max_bytes
    entries = payload.get("entries") or []
    dropped: list[str] = []
    i = len(entries) - 1
    while i >= 0 and _payload_bytes(payload) > limit:
        if "content" in entries[i]:
            del entries[i]["content"]
            dropped.append(_entry_key(entries[i]))
        i -= 1
    return dropped


def _reuse_prior_content(
    entries: list[dict], *, prior: dict[str, str], old_state: dict[str, dict]
) -> tuple[dict[str, dict], dict[str, int]]:
    """Phase-1 step: attach already-published text to the fresh entries (no
    network) and rebuild the per-source state for exactly these entries.

    The new state holds ONLY the keys of the current entries (entries that
    rolled off the feed are pruned) and never more than
    MAX_STATE_KEYS_PER_SOURCE of them."""
    stats = {"reused": 0, "fetched": 0, "failed": 0, "dropped_for_size": 0}
    new_state: dict[str, dict] = {}
    for entry in entries:
        key = _entry_key(entry)
        if not key or key in new_state or len(new_state) >= MAX_STATE_KEYS_PER_SOURCE:
            continue
        rec = dict(old_state.get(key) or {})
        if key in prior:
            entry["content"] = _finalize_content(prior[key])
            rec["status"] = "ok"
            rec["attempts"] = max(int(rec.get("attempts", 0) or 0), 1)
            rec.pop("reason", None)
            stats["reused"] += 1
        if rec:
            new_state[key] = rec
    return new_state, stats


def _needs_fetch(entry: dict, rec: dict | None) -> bool:
    if entry.get("content") or not entry.get("url"):
        return False
    if rec is None:
        return True
    if rec.get("status") == "dropped":
        return False
    return int(rec.get("attempts", 0) or 0) < CONTENT_MAX_ATTEMPTS


def _interleave(queues: list[list[tuple[str, dict]]]) -> list[tuple[str, dict]]:
    """Round-robin across sources so one source cannot spend the whole budget."""
    out: list[tuple[str, dict]] = []
    depth = max((len(q) for q in queues), default=0)
    for i in range(depth):
        for q in queues:
            if i < len(q):
                out.append(q[i])
    return out


def _content_phase(
    slots: dict[str, dict],
    *,
    budget: ContentBudget,
    host_spacing_s: float,
    flush,
) -> dict[str, Any]:
    """Fetch article text for entries that have none, within ``budget``.

    ``slots[source]`` = {"target", "payload", "state", "stats"}; entries are
    mutated in place and ``flush(source)`` persists that source's file+state
    after every attempt, so a kill loses at most the article in flight."""
    queues: list[list[tuple[str, dict]]] = []
    for source, slot in slots.items():
        if slot["target"].get("fetch_content", True) is False:
            continue
        queue = [
            (source, e)
            for e in slot["payload"]["entries"]
            if _needs_fetch(e, slot["state"].get(_entry_key(e)))
        ]
        if queue:
            queues.append(queue)

    last_hit: dict[str, float] = {}
    attempted = 0
    for source, entry in _interleave(queues):
        if not budget.has_room():
            break
        slot = slots[source]
        host = (urlparse(entry["url"]).hostname or "").lower()
        wait = host_spacing_s - (_clock() - last_hit[host]) if host in last_hit else 0.0
        if wait > 0:
            if wait >= budget.remaining():
                budget.exhausted = True
                break
            _sleep(wait)
        if not budget.take():
            break
        attempted += 1

        key = _entry_key(entry)
        attempts = int((slot["state"].get(key) or {}).get("attempts", 0) or 0)
        result = _fetch_article(
            entry["url"], timeout_s=min(CONTENT_FETCH_TIMEOUT_S, max(0.5, budget.remaining()))
        )
        last_hit[host] = _clock()
        text = _finalize_content(result.text)
        if text:
            entry["content"] = text
            slot["state"][key] = {"status": "ok", "attempts": attempts + 1}
            slot["stats"]["fetched"] += 1
        else:
            permanent = result.reason in _PERMANENT_REASONS
            slot["state"][key] = {
                "status": "empty",
                "attempts": CONTENT_MAX_ATTEMPTS if permanent else attempts + 1,
                "reason": result.reason,
            }
            slot["stats"]["failed"] += 1
        flush(source)
    return {"attempted": attempted}


def _load_targets(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    targets = cfg.get("targets") or []
    if not isinstance(targets, list):
        raise SystemExit(f"targets in {path} must be a list")
    return targets


def _write_payload(source: str, payload: dict, output_dir: Path) -> Path:
    dest = output_dir / f"{source}.json"
    _atomic_write_text(dest, _dump_payload(payload))
    return dest


def _failed_payload(target: dict, exc: BaseException) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "source": target["source"],
        "name": target.get("name", target["source"]),
        "feed_url": target["url"],
        "fetched_at": now.isoformat(),
        "next_refresh_at": (now + CRON_INTERVAL).isoformat(),
        "entries": [],
        "errors": [{"stage": "runner", "message": f"{type(exc).__name__}: {exc}"[:200]}],
    }


def _mark_dropped(state: dict[str, dict], keys: list[str]) -> None:
    for key in keys:
        prev = state.get(key) or {}
        state[key] = {"status": "dropped", "attempts": int(prev.get("attempts", 1) or 1)}


def run(
    *,
    targets_file: Path = TARGETS_FILE,
    output_dir: Path = OUTPUT_DIR,
    max_workers: int = 4,
    fetch_content: bool = True,
    content_budget_s: float = CONTENT_RUN_BUDGET_S,
    content_max_new: int = CONTENT_MAX_NEW_PER_RUN,
    content_host_spacing_s: float = CONTENT_HOST_SPACING_S,
) -> dict:
    """Fetch every target and write JSON. Returns a summary dict.

    Two phases. Phase 1 fetches every feed and writes every ``{source}.json``
    (carrying over article text published by earlier runs). Phase 2 - only
    with ``fetch_content`` - fetches article text for entries that have none,
    inside ``content_budget_s`` / ``content_max_new``, rewriting each file as
    it goes. Phase 2 can lose article text; it cannot lose phase 1's output
    and it never changes the exit status."""
    targets = _load_targets(targets_file)
    if not targets:
        print(f"no targets in {targets_file}", file=sys.stderr)
        return {"total": 0, "ok": 0, "failed": 0, "details": []}

    old_state = _load_state(output_dir) if fetch_content else {}
    slots: dict[str, dict] = {}
    details: list[dict] = []

    def _persist_state() -> None:
        _write_state({s: sl["state"] for s, sl in slots.items()}, output_dir)

    def _flush(source: str) -> None:
        slot = slots[source]
        dropped = _cap_payload_bytes(slot["payload"])
        _mark_dropped(slot["state"], dropped)
        slot["stats"]["dropped_for_size"] += len(dropped)
        _write_payload(source, slot["payload"], output_dir)
        _persist_state()

    _ROBOTS.clear()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_target, t): t for t in targets}
        for fut in as_completed(futures):
            target = futures[fut]
            try:
                payload = fut.result()
            except Exception as exc:  # noqa: BLE001 - record every failure
                payload = _failed_payload(target, exc)
            source = payload["source"]
            content_stats = None
            if fetch_content:
                if payload["entries"]:
                    state_for_source, content_stats = _reuse_prior_content(
                        payload["entries"],
                        prior=_load_prior_content(source, output_dir),
                        old_state=old_state.get(source) or {},
                    )
                    dropped = _cap_payload_bytes(payload)
                    _mark_dropped(state_for_source, dropped)
                    content_stats["dropped_for_size"] = len(dropped)
                else:
                    # A failed feed fetch must not forget what was attempted.
                    state_for_source = old_state.get(source) or {}
                    content_stats = {"reused": 0, "fetched": 0, "failed": 0, "dropped_for_size": 0}
                slots[source] = {
                    "target": target,
                    "payload": payload,
                    "state": state_for_source,
                    "stats": content_stats,
                }
            dest = _write_payload(source, payload, output_dir)
            try:
                rel = str(dest.relative_to(REPO_ROOT))
            except ValueError:
                rel = str(dest)
            details.append(
                {
                    "source": source,
                    "path": rel,
                    "entries": len(payload["entries"]),
                    "errors": len(payload["errors"]),
                    "content": content_stats,
                }
            )

    ok = sum(1 for d in details if d["entries"] > 0)
    failed = len(details) - ok

    content_info: dict[str, Any] | None = None
    budget: ContentBudget | None = None
    if fetch_content:
        started = _clock()
        content_info = {"attempted": 0, "error": None, "interrupted": False}
        try:
            _persist_state()
            # The budget clock starts here, after the bookkeeping write, so it
            # measures article fetching only.
            budget = ContentBudget(content_budget_s, content_max_new)
            content_info.update(
                _content_phase(
                    slots,
                    budget=budget,
                    host_spacing_s=content_host_spacing_s,
                    flush=_flush,
                )
            )
        except KeyboardInterrupt:
            # The step timeout's SIGINT: stop fetching, keep everything so far.
            content_info["interrupted"] = True
        except Exception as exc:  # noqa: BLE001 - content must never fail the run
            content_info["error"] = f"{type(exc).__name__}: {exc}"[:200]
            print(f"content phase aborted: {content_info['error']}", file=sys.stderr)
        finally:
            _close_http()
        try:
            _persist_state()
        except OSError as exc:
            print(f"could not write content state: {exc}", file=sys.stderr)
        content_info["elapsed_s"] = round(_clock() - started, 1)
        for d in details:
            slot = slots.get(d["source"])
            if slot and d["content"] is not None:
                d["content"]["without_content"] = sum(
                    1 for e in slot["payload"]["entries"] if not e.get("content")
                )

    summary = {
        "total": len(details),
        "ok": ok,
        "failed": failed,
        "content_budget_exhausted": bool(budget and budget.exhausted),
        "content_phase": content_info,
        "details": details,
    }
    print(json.dumps(summary, indent=2))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Athena feed mirror runner")
    parser.add_argument(
        "--targets",
        type=Path,
        default=TARGETS_FILE,
        help="Path to feed_mirror_targets.yaml",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Directory to write {source}.json files into",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="ThreadPoolExecutor size for parallel fetches",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Reserved - there's no daemon mode today; --once is a no-op marker.",
    )
    parser.add_argument(
        "--no-content",
        action="store_true",
        help="Do not fetch article text (KP-R11); writes the pre-KP-R11 contract.",
    )
    parser.add_argument(
        "--content-budget-s",
        type=float,
        default=CONTENT_RUN_BUDGET_S,
        help="Wall-clock budget for the whole article-text phase (seconds).",
    )
    parser.add_argument(
        "--content-max-new",
        type=int,
        default=CONTENT_MAX_NEW_PER_RUN,
        help="Maximum article fetch attempts per run.",
    )
    args = parser.parse_args(argv)
    summary = run(
        targets_file=args.targets,
        output_dir=args.output_dir,
        max_workers=args.max_workers,
        fetch_content=not args.no_content,
        content_budget_s=args.content_budget_s,
        content_max_new=args.content_max_new,
    )
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
