"""Test isolation for the mirror runner.

No test may fetch an article body from the network, sleep for real, or read a
clock it did not set: tests that exercise content fetching install their own
stub (``_fetch_article``) or run the real function against a local server.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import run_feed_mirror as runner  # noqa: E402


@pytest.fixture(autouse=True)
def _no_live_article_fetch(monkeypatch):
    monkeypatch.setattr(runner, "_fetch_article", lambda url, **kw: runner.ArticleResult("", "empty"))
    monkeypatch.setattr(runner, "_sleep", lambda seconds: None)
    runner._ROBOTS.clear()
    yield
    runner._ROBOTS.clear()
