"""Unit tests for the feed mirror runner.

External feedparser / HTML fetches are mocked. Tests pin the JSON
shape that Athena's ``fetch_mirrored_feed`` primitive will consume.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import run_feed_mirror as runner  # noqa: E402


def _fake_entry(title="Post", link="https://x.example/1"):
    return {
        "title": title,
        "summary": "<p>Body content with <b>html</b></p>",
        "link": link,
        "id": title.lower().replace(" ", "-"),
        "published": "Mon, 01 Jan 2024 00:00:00 GMT",
        "author": "Editor",
    }


def test_clean_strips_html_and_whitespace():
    assert runner._clean("<p>Hello   <b>world</b></p>") == "Hello world"
    assert runner._clean("") == ""


def test_autodiscover_parses_rss_and_atom_links():
    html = (
        '<link rel="alternate" type="application/rss+xml" href="/feed/" />'
        '<link rel="alternate" type="application/atom+xml" '
        'href="https://x.example/atom" />'
        '<link rel="stylesheet" href="/css">'
    )
    urls = runner._autodiscover_feed_urls(html, "https://site.example/")
    assert urls == ["https://site.example/feed/", "https://x.example/atom"]


def test_fetch_target_writes_contract_shape(tmp_path):
    target = {
        "source": "test-source",
        "name": "Test Source",
        "url": "https://test.example/feed",
        "max_entries": 5,
    }
    fake_feed = SimpleNamespace(entries=[_fake_entry("Hello world")])
    with patch.object(runner, "feedparser") as fp:
        fp.parse.return_value = fake_feed
        payload = runner._fetch_target(target)

    # Contract fields — every consumer reads these by name.
    assert payload["source"] == "test-source"
    assert payload["name"] == "Test Source"
    assert payload["feed_url"] == "https://test.example/feed"
    assert "fetched_at" in payload
    assert "next_refresh_at" in payload
    assert payload["errors"] == []

    assert len(payload["entries"]) == 1
    entry = payload["entries"][0]
    assert entry["title"] == "Hello world"
    assert entry["url"] == "https://x.example/1"
    assert entry["author"] == "Editor"
    # Body cleaned to plain text.
    assert "Body content" in entry["summary"]
    assert "<p>" not in entry["summary"]


def test_fetch_target_records_error_when_all_urls_empty(tmp_path):
    target = {
        "source": "dead-source",
        "name": "Dead Source",
        "url": "https://dead.example/feed",
        "fallback_urls": ["https://dead.example/rss"],
    }
    empty = SimpleNamespace(entries=[])
    with (
        patch.object(runner, "feedparser") as fp,
        patch.object(runner, "_fetch_html", return_value=""),
    ):
        fp.parse.return_value = empty
        payload = runner._fetch_target(target)

    assert payload["entries"] == []
    assert any(e["stage"] == "fetch" for e in payload["errors"])


def test_run_writes_one_json_per_target(tmp_path):
    targets_file = tmp_path / "targets.yaml"
    targets_file.write_text(
        "targets:\n"
        "  - source: a\n"
        "    name: A\n"
        "    url: https://a.example/feed\n"
        "  - source: b\n"
        "    name: B\n"
        "    url: https://b.example/feed\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "feed_mirror"

    fake = SimpleNamespace(entries=[_fake_entry("Post 1", "https://x.example/1")])
    with patch.object(runner, "feedparser") as fp:
        fp.parse.return_value = fake
        summary = runner.run(
            targets_file=targets_file,
            output_dir=output_dir,
            max_workers=2,
        )

    assert summary["total"] == 2
    assert summary["ok"] == 2
    assert (output_dir / "a.json").exists()
    assert (output_dir / "b.json").exists()
    payload_a = json.loads((output_dir / "a.json").read_text(encoding="utf-8"))
    assert payload_a["source"] == "a"
    assert len(payload_a["entries"]) == 1


def test_run_isolated_failure_does_not_drop_other_targets(tmp_path):
    """One target raising must not block the others — same isolation
    invariant as Athena's rss_blog v2."""
    targets_file = tmp_path / "targets.yaml"
    targets_file.write_text(
        "targets:\n"
        "  - source: good\n"
        "    name: Good\n"
        "    url: https://good.example/feed\n"
        "  - source: bad\n"
        "    name: Bad\n"
        "    url: https://bad.example/feed\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "feed_mirror"

    good_feed = SimpleNamespace(entries=[_fake_entry("Good")])

    def _parse(url, **_kwargs):
        if "bad" in url:
            raise RuntimeError("connection blew up")
        return good_feed

    with patch.object(runner, "feedparser") as fp:
        fp.parse.side_effect = _parse
        runner.run(
            targets_file=targets_file,
            output_dir=output_dir,
            max_workers=2,
        )

    assert (output_dir / "good.json").exists()
    assert (output_dir / "bad.json").exists()
    payload_bad = json.loads((output_dir / "bad.json").read_text(encoding="utf-8"))
    assert payload_bad["entries"] == []
    assert any("runner" in e["stage"] or "connection" in e["message"]
               for e in payload_bad["errors"])
