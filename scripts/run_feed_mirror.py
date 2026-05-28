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
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
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
CRON_INTERVAL = timedelta(minutes=30)

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


def _fetch_html(url: str) -> str:
    """Best-effort HTML fetch for autodiscovery; returns '' on any failure."""
    try:
        import httpx
    except ImportError:
        return ""
    try:
        resp = httpx.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*"},
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
    return {
        "title": title.strip(),
        "url": link.strip(),
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


def _load_targets(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    targets = cfg.get("targets") or []
    if not isinstance(targets, list):
        raise SystemExit(f"targets in {path} must be a list")
    return targets


def _write_payload(source: str, payload: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    dest = output_dir / f"{source}.json"
    dest.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    return dest


def run(
    *,
    targets_file: Path = TARGETS_FILE,
    output_dir: Path = OUTPUT_DIR,
    max_workers: int = 4,
) -> dict:
    """Fetch every target and write JSON. Returns a summary dict."""
    targets = _load_targets(targets_file)
    if not targets:
        print(f"no targets in {targets_file}", file=sys.stderr)
        return {"total": 0, "ok": 0, "failed": 0, "details": []}

    details: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_target, t): t for t in targets}
        for fut in as_completed(futures):
            target = futures[fut]
            try:
                payload = fut.result()
            except Exception as exc:  # noqa: BLE001 — record every failure
                payload = {
                    "source": target["source"],
                    "name": target.get("name", target["source"]),
                    "feed_url": target["url"],
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "next_refresh_at": (
                        datetime.now(timezone.utc) + CRON_INTERVAL
                    ).isoformat(),
                    "entries": [],
                    "errors": [{"stage": "runner", "message": f"{type(exc).__name__}: {exc}"[:200]}],
                }
            dest = _write_payload(payload["source"], payload, output_dir)
            try:
                rel = str(dest.relative_to(REPO_ROOT))
            except ValueError:
                rel = str(dest)
            details.append(
                {
                    "source": payload["source"],
                    "path": rel,
                    "entries": len(payload["entries"]),
                    "errors": len(payload["errors"]),
                }
            )

    ok = sum(1 for d in details if d["entries"] > 0)
    failed = len(details) - ok
    summary = {"total": len(details), "ok": ok, "failed": failed, "details": details}
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
        help="Reserved — there's no daemon mode today; --once is a no-op marker.",
    )
    args = parser.parse_args(argv)
    summary = run(
        targets_file=args.targets,
        output_dir=args.output_dir,
        max_workers=args.max_workers,
    )
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
