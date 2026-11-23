"""Article text in the mirror JSON (KP-R11, 2026-10).

Pins, in order: the output contract Athena's ``fetch_mirrored_feed`` consumes,
incremental state, the per-run caps, the guarantee that the feed update is
written before (and survives) any content failure, and the real network
function against a local server.
"""

from __future__ import annotations

import http.server
import json
import os
import socket
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import run_feed_mirror as runner

_REAL_FETCH = runner._fetch_article  # captured before conftest's autouse stub


def _entry(n, host="x.example"):
    return {
        "title": f"Post {n}",
        "summary": "<p>summary</p>",
        "link": f"https://{host}/{n}",
        "id": f"tag:{host},{n}",
        "published": "Mon, 01 Jan 2024 00:00:00 GMT",
        "author": "Editor",
    }


def _targets_file(tmp_path, specs):
    """specs: {source: {"host": ..., "extra yaml lines": ...}}"""
    lines = ["targets:"]
    for source, extra in specs.items():
        lines += [
            f"  - source: {source}",
            f"    name: {source.upper()}",
            f"    url: https://{source}.example/feed",
        ]
        lines += [f"    {x}" for x in extra]
    f = tmp_path / "targets.yaml"
    f.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return f


def _run_multi(tmp_path, feeds, extras=None, **kw):
    """feeds: {source: [raw entries]}; returns (summary, {source: payload}, state)."""
    out = tmp_path / "out"

    def _parse(url, **_k):
        for source, entries in feeds.items():
            if url == f"https://{source}.example/feed":
                return SimpleNamespace(entries=entries)
        return SimpleNamespace(entries=[])

    with patch.object(runner, "feedparser") as fp:
        fp.parse.side_effect = _parse
        summary = runner.run(
            targets_file=_targets_file(tmp_path, {s: (extras or {}).get(s, []) for s in feeds}),
            output_dir=out,
            max_workers=1,
            **kw,
        )
    payloads = {s: json.loads((out / f"{s}.json").read_text(encoding="utf-8")) for s in feeds}
    state_file = out / "_state" / "content_state.json"
    state = json.loads(state_file.read_text(encoding="utf-8")) if state_file.exists() else None
    return summary, payloads, state


def _run(tmp_path, entries, **kw):
    summary, payloads, state = _run_multi(tmp_path, {"a": entries}, **kw)
    return summary, payloads["a"], state


def _ok(text="Body. " * 60):
    return runner.ArticleResult(text, "ok")


def _stub(monkeypatch, result_for):
    calls: list[str] = []

    def _fake(url, **kw):
        calls.append(url)
        return result_for(url)

    monkeypatch.setattr(runner, "_fetch_article", _fake)
    return calls


# ── output contract ──────────────────────────────────────────────────────────


def test_entry_dict_carries_id_with_url_fallback():
    assert runner._entry_to_dict(_entry(1))["id"] == "tag:x.example,1"
    no_id = {k: v for k, v in _entry(2).items() if k != "id"}
    assert runner._entry_to_dict(no_id)["id"] == "https://x.example/2"


def test_scraped_entries_carry_id():
    target = {
        "source": "s",
        "name": "S",
        "url": "https://s.example/blog",
        "scrape": {"index_url": "https://s.example/blog", "link_prefix": "/blog/", "enrich": False},
    }
    with patch.object(runner, "_fetch_html", return_value='<a href="/blog/one">One</a>'):
        entries, _errors, _ = runner._scrape_index(target)
    assert entries[0]["id"] == entries[0]["url"] == "https://s.example/blog/one"


def test_output_matches_mirrored_feed_contract(tmp_path, monkeypatch):
    """What fetch_mirrored_feed (HV-MCP 4bf97d2d) reads: entries[].title /
    url non-empty strings, optional id, and content as a plain string of at
    most FULLTEXT_TEXT_MAX_CHARS (32_000), already stripped."""
    _stub(monkeypatch, lambda url: _ok("  Lead paragraph.\n\n" + "More text. " * 4000 + "  "))
    _s, payload, _st = _run(tmp_path, [_entry(1), _entry(2)])
    assert runner.CONTENT_MAX_CHARS == 32_000
    for key in ("source", "name", "feed_url", "fetched_at", "next_refresh_at", "entries", "errors"):
        assert key in payload
    for e in payload["entries"]:
        assert isinstance(e["id"], str) and e["id"]
        assert e["title"].strip() and e["url"].startswith("https://")
        assert isinstance(e["content"], str)
        assert e["content"] == e["content"].strip()
        assert 0 < len(e["content"]) <= 32_000


def test_content_key_absent_not_null_when_unavailable(tmp_path):
    _s, payload, _st = _run(tmp_path, [_entry(1)])  # conftest stub: empty
    assert "content" not in payload["entries"][0]


def test_consumer_reads_content_end_to_end(tmp_path, monkeypatch):
    """Run the REAL consumer (when importable) against files this runner wrote."""
    mf = pytest.importorskip("hv_primitives.research_sources.mirrored_feed")
    if "fulltext" not in Path(mf.__file__).read_text(encoding="utf-8"):
        pytest.skip("installed hv_primitives predates the content contract (HV-MCP 4bf97d2d)")
    body = "Real article text. " * 80
    _stub(monkeypatch, lambda url: _ok(body))
    _run(tmp_path, [_entry(1), _entry(2)])
    handler = lambda *a, **k: http.server.SimpleHTTPRequestHandler(  # noqa: E731
        *a, directory=str(tmp_path / "out"), **k
    )
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        res = mf.fetch_mirrored_feed(
            sources=["a"],
            domain="probe",
            options={"base_url": f"http://127.0.0.1:{srv.server_port}", "min_score": 0.0},
        )
    finally:
        srv.shutdown()
    assert res.ok and len(res.data) == 2
    assert all(r["metadata"]["fulltext"]["text"] == body.strip() for r in res.data)


# ── incremental state ────────────────────────────────────────────────────────


def test_run_attaches_content_and_writes_state(tmp_path, monkeypatch):
    _stub(monkeypatch, lambda url: _ok(f"Body of {url}. " * 50))
    summary, payload, state = _run(tmp_path, [_entry(1), _entry(2)])
    assert all(e["content"].startswith("Body of https://x.example/") for e in payload["entries"])
    assert state["a"]["tag:x.example,1"] == {"status": "ok", "attempts": 1}
    assert summary["details"][0]["content"]["fetched"] == 2


def test_second_run_reuses_content_without_refetch(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: _ok())
    _run(tmp_path, [_entry(1)])
    summary, payload, _state = _run(tmp_path, [_entry(1)])
    assert calls == ["https://x.example/1"]
    assert payload["entries"][0]["content"] == ("Body. " * 60).strip()
    assert summary["details"][0]["content"]["reused"] == 1


def test_failed_fetch_is_retried_at_most_three_times(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: runner.ArticleResult("", "timeout"))
    for _ in range(5):
        _s, payload, state = _run(tmp_path, [_entry(1)])
    assert len(calls) == runner.CONTENT_MAX_ATTEMPTS == 3
    assert state["a"]["tag:x.example,1"] == {"status": "empty", "attempts": 3, "reason": "timeout"}
    assert "content" not in payload["entries"][0]


def test_permanent_failure_is_not_retried(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: runner.ArticleResult("", "http_forbidden"))
    for _ in range(3):
        _s, _p, state = _run(tmp_path, [_entry(1)])
    assert len(calls) == 1
    assert state["a"]["tag:x.example,1"]["attempts"] == runner.CONTENT_MAX_ATTEMPTS


def test_state_is_pruned_to_current_entries(tmp_path, monkeypatch):
    _stub(monkeypatch, lambda url: _ok())
    state_dir = tmp_path / "out" / "_state"
    state_dir.mkdir(parents=True)
    (state_dir / "content_state.json").write_text(
        json.dumps(
            {
                "a": {"tag:x.example,gone": {"status": "ok", "attempts": 1}},
                "removed-source": {"k": {"status": "ok", "attempts": 1}},
            }
        ),
        encoding="utf-8",
    )
    _s, _p, state = _run(tmp_path, [_entry(1)])
    assert set(state) == {"a"}
    assert set(state["a"]) == {"tag:x.example,1"}


def test_failed_feed_keeps_attempt_history(tmp_path, monkeypatch):
    _stub(monkeypatch, lambda url: runner.ArticleResult("", "timeout"))
    _run(tmp_path, [_entry(1)])
    with patch.object(runner, "_fetch_html", return_value=""):
        _s, payload, state = _run(tmp_path, [])  # the feed returned nothing
    assert payload["entries"] == []
    assert state["a"]["tag:x.example,1"]["attempts"] == 1


def test_corrupt_state_file_is_ignored(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: _ok())
    state_dir = tmp_path / "out" / "_state"
    state_dir.mkdir(parents=True)
    (state_dir / "content_state.json").write_text("{not json", encoding="utf-8")
    _s, payload, _st = _run(tmp_path, [_entry(1)])
    assert len(calls) == 1 and "content" in payload["entries"][0]


def test_state_records_are_validated_and_bounded():
    raw = {
        "a": {f"k{i}": {"status": "ok", "attempts": 1} for i in range(runner.MAX_STATE_KEYS_PER_SOURCE + 200)},
        "b": {"bad-status": {"status": "weird", "attempts": 1}, "bad-attempts": {"status": "ok", "attempts": "x"}},
        "c": "not a dict",
    }
    clean = runner._sanitize_state(raw)
    assert len(clean["a"]) == runner.MAX_STATE_KEYS_PER_SOURCE
    assert clean["b"] == {}
    assert "c" not in clean


def test_state_for_a_huge_feed_is_bounded():
    entries = [{"id": f"id{i}", "url": f"https://x.example/{i}"} for i in range(runner.MAX_STATE_KEYS_PER_SOURCE + 50)]
    new_state, _stats = runner._reuse_prior_content(
        entries, prior={e["id"]: "t" for e in entries}, old_state={}
    )
    assert len(new_state) == runner.MAX_STATE_KEYS_PER_SOURCE


def test_overlong_entry_id_is_hashed():
    key = runner._entry_key({"id": "u" * 5000, "url": "https://x.example/1"})
    assert key.startswith("sha1:") and len(key) < 60


def test_cap_payload_bytes_drops_oldest_content_first():
    entries = [{"id": f"k{i}", "url": f"u{i}", "content": "c" * 1000} for i in range(3)]
    payload = {"source": "a", "entries": entries}
    only_first = {
        "source": "a",
        "entries": [entries[0], {"id": "k1", "url": "u1"}, {"id": "k2", "url": "u2"}],
    }
    limit = runner._payload_bytes(only_first)
    dropped = runner._cap_payload_bytes(json.loads(json.dumps(payload)), max_bytes=limit)
    assert dropped == ["k2", "k1"]


def test_dropped_content_is_not_refetched(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: _ok("w" * 1_000))
    entries = [_entry(1), _entry(2), _entry(3)]
    (tmp_path / "probe").mkdir()
    (tmp_path / "real").mkdir()
    # Size of the file when only the NEWEST entry keeps its content.
    _run(tmp_path / "probe", entries, fetch_content=False)
    probe = json.loads((tmp_path / "probe" / "out" / "a.json").read_text(encoding="utf-8"))
    probe["entries"][0]["content"] = "w" * 1_000
    monkeypatch.setattr(runner, "MAX_PAYLOAD_BYTES", runner._payload_bytes(probe) + 50)
    _s, payload, state = _run(tmp_path / "real", entries)
    assert [("content" in e) for e in payload["entries"]] == [True, False, False]
    assert state["a"]["tag:x.example,3"]["status"] == "dropped"
    n = len(calls)
    _run(tmp_path / "real", entries)
    assert len(calls) == n  # nothing fetched again


# ── caps and budget ──────────────────────────────────────────────────────────


def test_count_budget_caps_attempts_and_reports_exhaustion(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: _ok())
    summary, payload, _st = _run(tmp_path, [_entry(1), _entry(2), _entry(3)], content_max_new=1)
    assert len(calls) == 1
    assert summary["content_budget_exhausted"] is True
    assert sum(1 for e in payload["entries"] if "content" in e) == 1
    _run(tmp_path, [_entry(1), _entry(2), _entry(3)], content_max_new=1)  # next run: next one
    assert len(calls) == 2


def test_failed_attempts_count_against_the_budget(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: runner.ArticleResult("", "timeout"))
    _run(tmp_path, [_entry(i) for i in range(10)], content_max_new=4)
    assert len(calls) == 4


def test_budget_unit_count_and_deadline():
    now = [0.0]
    b = runner.ContentBudget(10, 2, clock=lambda: now[0])
    assert b.take() and b.take() and not b.take() and b.exhausted
    now[0] = 0.0
    b2 = runner.ContentBudget(10, 99, clock=lambda: now[0])
    assert b2.take()
    now[0] = 10.0
    assert not b2.take() and b2.exhausted and b2.remaining() == 0.0


def test_run_stops_fetching_when_time_budget_is_spent(tmp_path, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(runner, "_clock", lambda: now[0])

    def _slow(url, **kw):
        now[0] += 50.0
        return _ok()

    calls = _stub(monkeypatch, lambda url: _slow(url))
    summary, _p, _st = _run(tmp_path, [_entry(i) for i in range(10)], content_budget_s=120)
    assert len(calls) == 3  # 0 -> 50 -> 100 -> 150: the fourth finds the budget gone
    assert summary["content_budget_exhausted"] is True


def test_item_timeout_never_exceeds_remaining_budget(tmp_path, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(runner, "_clock", lambda: now[0])
    seen = []

    def _fake(url, *, timeout_s, **kw):
        seen.append(timeout_s)
        now[0] += 7.0
        return _ok()

    monkeypatch.setattr(runner, "_fetch_article", _fake)
    _run(tmp_path, [_entry(1), _entry(2)], content_budget_s=10)
    assert seen[0] == 10.0 and seen[1] == pytest.approx(3.0)


def test_sources_are_interleaved_so_one_cannot_eat_the_budget(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: _ok())
    feeds = {"a": [_entry(i, "a.example") for i in range(5)], "b": [_entry(i, "b.example") for i in range(5)]}
    _s, payloads, _st = _run_multi(tmp_path, feeds, content_max_new=2)
    assert sorted(calls) == ["https://a.example/0", "https://b.example/0"]
    assert "content" in payloads["a"]["entries"][0] and "content" in payloads["b"]["entries"][0]


def test_same_host_requests_are_spaced(tmp_path, monkeypatch):
    now = [0.0]
    slept: list[float] = []
    monkeypatch.setattr(runner, "_clock", lambda: now[0])
    monkeypatch.setattr(runner, "_sleep", lambda s: (slept.append(s), now.__setitem__(0, now[0] + s)))
    _stub(monkeypatch, lambda url: _ok())
    _run(tmp_path, [_entry(1), _entry(2)], content_host_spacing_s=2.0)
    assert slept == [pytest.approx(2.0)]


def test_different_hosts_are_not_delayed(tmp_path, monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(runner, "_sleep", lambda s: slept.append(s))
    _stub(monkeypatch, lambda url: _ok())
    feeds = {"a": [_entry(1, "a.example")], "b": [_entry(1, "b.example")]}
    _run_multi(tmp_path, feeds, content_host_spacing_s=2.0)
    assert slept == []


def test_spacing_that_would_outlast_the_budget_ends_the_phase(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "_clock", lambda: 0.0)  # frozen: no wall-clock flake
    calls = _stub(monkeypatch, lambda url: _ok())
    summary, _p, _st = _run(
        tmp_path, [_entry(1), _entry(2), _entry(3)], content_budget_s=1, content_host_spacing_s=5.0
    )
    assert len(calls) == 1 and summary["content_budget_exhausted"] is True


def test_per_target_opt_out(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: _ok())
    feeds = {"a": [_entry(1, "a.example")], "b": [_entry(1, "b.example")]}
    _s, payloads, _st = _run_multi(tmp_path, feeds, extras={"b": ["fetch_content: false"]})
    assert calls == ["https://a.example/1"]
    assert "content" not in payloads["b"]["entries"][0]


def test_no_content_flag_keeps_old_contract(tmp_path, monkeypatch):
    calls = _stub(monkeypatch, lambda url: _ok())
    summary, payload, state = _run(tmp_path, [_entry(1)], fetch_content=False)
    assert calls == []
    assert "content" not in payload["entries"][0]
    assert state is None and summary["content_phase"] is None


# ── the feed update is written first and survives any content failure ───────


def test_feed_files_exist_before_the_first_article_fetch(tmp_path, monkeypatch):
    out = tmp_path / "out"
    seen = {}

    def _fake(url, **kw):
        seen.setdefault("files", sorted(p.name for p in out.glob("*.json")))
        return _ok()

    monkeypatch.setattr(runner, "_fetch_article", _fake)
    feeds = {"a": [_entry(1, "a.example")], "b": [_entry(1, "b.example")]}
    _run_multi(tmp_path, feeds)
    assert seen["files"] == ["a.json", "b.json"]


def test_unexpected_content_error_keeps_the_feed_update(tmp_path, monkeypatch):
    def _boom(url, **kw):
        raise RuntimeError("trafilatura exploded")

    monkeypatch.setattr(runner, "_fetch_article", _boom)
    summary, payload, _st = _run(tmp_path, [_entry(1), _entry(2)])
    assert [e["title"] for e in payload["entries"]] == ["Post 1", "Post 2"]
    assert summary["ok"] == 1 and summary["failed"] == 0
    assert "trafilatura exploded" in summary["content_phase"]["error"]


def test_earlier_content_survives_a_run_whose_content_phase_dies(tmp_path, monkeypatch):
    _stub(monkeypatch, lambda url: _ok("kept text. " * 40))
    _run(tmp_path, [_entry(1)])

    def _boom(url, **kw):
        raise RuntimeError("down")

    monkeypatch.setattr(runner, "_fetch_article", _boom)
    _s, payload, _st = _run(tmp_path, [_entry(1), _entry(2)])
    assert payload["entries"][0]["content"].startswith("kept text.")
    assert [e["title"] for e in payload["entries"]] == ["Post 1", "Post 2"]


def test_interrupt_keeps_feed_and_content_fetched_so_far(tmp_path, monkeypatch):
    n = {"i": 0}

    def _fake(url, **kw):
        n["i"] += 1
        if n["i"] == 2:
            raise KeyboardInterrupt  # the step timeout's SIGINT
        return _ok("first article. " * 30)

    monkeypatch.setattr(runner, "_fetch_article", _fake)
    summary, payload, state = _run(tmp_path, [_entry(1), _entry(2), _entry(3)])
    assert summary["content_phase"]["interrupted"] is True
    assert payload["entries"][0]["content"].startswith("first article.")
    assert "content" not in payload["entries"][1]
    assert len(payload["entries"]) == 3
    assert state["a"]["tag:x.example,1"]["status"] == "ok"


def test_content_never_changes_the_exit_status(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "_fetch_article", lambda url, **kw: (_ for _ in ()).throw(RuntimeError("x")))
    targets = _targets_file(tmp_path, {"a": []})
    with patch.object(runner, "feedparser") as fp:
        fp.parse.return_value = SimpleNamespace(entries=[_entry(1)])
        rc = runner.main(["--targets", str(targets), "--output-dir", str(tmp_path / "out")])
    assert rc == 0


def test_write_is_atomic_a_failed_replace_leaves_the_old_file(tmp_path, monkeypatch):
    dest = tmp_path / "out" / "a.json"
    dest.parent.mkdir()
    dest.write_text('{"old": true}\n', encoding="utf-8")

    def _fail(src, dst):
        raise OSError("disk went away")

    monkeypatch.setattr(os, "replace", _fail)
    with pytest.raises(OSError):
        runner._write_payload("a", {"new": True}, dest.parent)
    assert json.loads(dest.read_text(encoding="utf-8")) == {"old": True}
    assert [p.name for p in dest.parent.iterdir()] == ["a.json"]


# ── the real fetch function, against a local server ─────────────────────────

_PARA = (
    "NAND flash vendors are shifting capacity toward enterprise SSDs as AI inference "
    "demand grows, and controller makers are following the same curve closely. "
)
_ARTICLE = (
    "<html><head><title>T</title></head><body><nav><a href='/'>Home</a> | <a href='/x'>About</a></nav>"
    "<article><h1>Headline of the day</h1>"
    + "".join(f"<p>{_PARA}Paragraph number {i}.</p>" for i in range(8))
    + "</article><footer>Copyright boilerplate footer</footer></body></html>"
)


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):  # silence
        pass

    def _send(self, code, body=b"", ctype="text/html; charset=utf-8", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        p = self.path
        if p == "/robots.txt":
            self._send(200, b"User-agent: *\nDisallow: /private\n", "text/plain")
        elif p in ("/article", "/private"):
            self._send(200, _ARTICLE.encode())
        elif p == "/short":
            self._send(200, b"<html><body><p>Subscribe to read this story.</p></body></html>")
        elif p == "/pdf":
            self._send(200, b"%PDF-1.4", "application/pdf")
        elif p == "/gone":
            self._send(404, b"nope")
        elif p == "/forbidden":
            self._send(403, b"no")
        elif p == "/oops":
            self._send(503, b"later")
        elif p == "/redir":
            self._send(302, b"", extra={"Location": "/article"})
        elif p == "/redir-private":
            self._send(302, b"", extra={"Location": "http://169.254.169.254/latest/meta-data/"})
        elif p == "/loop":
            self._send(302, b"", extra={"Location": "/loop"})
        elif p == "/huge":
            self._send(200, ("<html><body>" + "<p>" + _PARA * 40 + "</p>" * 1 + "</body></html>").encode() * 200)
        elif p == "/drip":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                for _ in range(100):
                    self.wfile.write(b"<p>drip drip drip</p>")
                    self.wfile.flush()
                    time.sleep(0.25)
            except OSError:
                pass
        else:
            self._send(404, b"?")


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setattr(runner, "_fetch_article", _REAL_FETCH)
    monkeypatch.setattr(runner, "_ALLOWED_PRIVATE_HOSTS", {"127.0.0.1"})
    runner._http()  # building the client loads the CA bundle (seconds on a scanned box); keep it out of timings
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def test_real_fetch_extracts_article_text_without_boilerplate(server):
    r = runner._fetch_article(f"{server}/article")
    assert r.reason == "ok"
    assert "Paragraph number 3." in r.text
    assert "Copyright boilerplate footer" not in r.text
    assert 200 < len(r.text) <= runner.CONTENT_MAX_CHARS


def test_real_fetch_follows_a_redirect(server):
    assert runner._fetch_article(f"{server}/redir").reason == "ok"


@pytest.mark.parametrize(
    ("path", "reason"),
    [
        ("/gone", "http_gone"),
        ("/forbidden", "http_forbidden"),
        ("/oops", "http_retry"),
        ("/short", "too_short"),
        ("/pdf", "not_html"),
        ("/private", "robots"),
        ("/loop", "http_client_error"),
        ("/redir-private", "blocked_host"),
    ],
)
def test_real_fetch_failure_reasons(server, path, reason):
    r = runner._fetch_article(f"{server}{path}")
    assert (r.text, r.reason) == ("", reason)


def test_permanent_vs_transient_classification():
    assert {"http_gone", "http_forbidden", "not_html", "robots", "too_short", "blocked_host"} <= runner._PERMANENT_REASONS
    assert not {"http_retry", "timeout", "network", "empty", "extract_error"} & runner._PERMANENT_REASONS


def test_real_fetch_connection_refused_is_network(server):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    r = runner._fetch_article(f"http://127.0.0.1:{port}/x")
    assert r.reason == "network"


def test_real_fetch_total_timeout_bounds_a_slow_drip(server):
    t0 = time.monotonic()
    r = runner._fetch_article(f"{server}/drip", timeout_s=1.0)
    assert r.reason == "timeout" and r.text == ""
    assert time.monotonic() - t0 < 8.0  # 1 s budget + scheduling slack; the drip alone would last 25 s


def test_real_fetch_reads_at_most_the_byte_cap(server, monkeypatch):
    import trafilatura

    seen = []

    def _spy(data, **kw):
        seen.append(len(data))
        return "x" * 500

    monkeypatch.setattr(trafilatura, "extract", _spy)
    monkeypatch.setattr(runner, "CONTENT_MAX_BYTES", 20_000)
    assert runner._fetch_article(f"{server}/huge").reason == "ok"
    assert seen == [20_000]


def test_real_fetch_caps_text_at_32000_chars(server, monkeypatch):
    import trafilatura

    monkeypatch.setattr(trafilatura, "extract", lambda data, **kw: "z" * 40_000)
    r = runner._fetch_article(f"{server}/article")
    assert len(r.text) == runner.CONTENT_MAX_CHARS == 32_000


def test_trafilatura_exception_is_a_failed_item_not_a_crash(server, monkeypatch):
    import trafilatura

    def _boom(data, **kw):
        raise ValueError("lxml choked")

    monkeypatch.setattr(trafilatura, "extract", _boom)
    assert runner._fetch_article(f"{server}/article") == ("", "extract_error")


def test_trafilatura_none_is_empty_and_retryable(server, monkeypatch):
    import trafilatura

    monkeypatch.setattr(trafilatura, "extract", lambda data, **kw: None)
    r = runner._fetch_article(f"{server}/article")
    assert r == ("", "empty") and r.reason not in runner._PERMANENT_REASONS


def test_trafilatura_failure_does_not_fail_the_run(tmp_path, monkeypatch):
    """End to end through run(): the real fetch function with a failing
    extractor must leave the feed intact and mark the entry retryable."""
    import trafilatura

    monkeypatch.setattr(runner, "_fetch_article", _REAL_FETCH)
    monkeypatch.setattr(runner, "_url_block_reason", lambda url: None)
    monkeypatch.setattr(runner, "_robots_allows", lambda url, **kw: True)

    class _Resp:
        status_code = 200
        headers = {"content-type": "text/html"}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def iter_bytes(self):
            yield b"<html>x</html>"

    monkeypatch.setattr(runner, "_http", lambda: SimpleNamespace(stream=lambda *a, **k: _Resp()))
    monkeypatch.setattr(trafilatura, "extract", lambda d, **k: (_ for _ in ()).throw(ValueError("bad")))
    summary, payload, state = _run(tmp_path, [_entry(1)])
    assert payload["entries"][0]["title"] == "Post 1" and "content" not in payload["entries"][0]
    assert state["a"]["tag:x.example,1"] == {"status": "empty", "attempts": 1, "reason": "extract_error"}
    assert summary["failed"] == 0


# ── SSRF guard ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/x",
        "http://[::1]/x",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.1.2.3/",
        "http://192.168.0.10/",
        "http://localhost/",
    ],
)
def test_private_and_loopback_addresses_are_blocked(url):
    assert runner._url_block_reason(url) == "blocked_host"


@pytest.mark.parametrize("url", ["ftp://example.com/x", "file:///etc/passwd", "http:///x", "javascript:alert(1)"])
def test_non_http_urls_are_blocked(url):
    assert runner._url_block_reason(url) == "blocked_url"


def test_hostname_resolving_to_a_private_address_is_blocked(monkeypatch):
    def _gai(host, port, *a, **k):
        ip = {"public.test": "93.184.216.34", "evil.test": "10.0.0.7"}[host]
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0))]

    monkeypatch.setattr(socket, "getaddrinfo", _gai)
    assert runner._url_block_reason("https://public.test/a") is None
    assert runner._url_block_reason("https://evil.test/a") == "blocked_host"


def test_unresolvable_host_is_transient(monkeypatch):
    def _gai(*a, **k):
        raise socket.gaierror("nope")

    monkeypatch.setattr(socket, "getaddrinfo", _gai)
    assert runner._url_block_reason("https://nx.test/a") == "network"
